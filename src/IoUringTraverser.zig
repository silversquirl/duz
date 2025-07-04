const IoUringTraverser = @This();

const uring_queue_size = 4096;

gpa: std.mem.Allocator,
arena: std.heap.ArenaAllocator.State,
root: std.fs.Dir,

uring: linux.IoUring,
stat_bufs: Store(u28, linux.Statx),
outstanding_tasks: u32,

output: std.ArrayListUnmanaged(Result),

pub fn init(general_purpose_alloc: std.mem.Allocator, root: std.fs.Dir) !IoUringTraverser {
    return .{
        .gpa = general_purpose_alloc,
        .arena = .{},
        .root = root,

        // OPTIM: IOPOLL, SQPOLL
        .uring = try linux.IoUring.init(uring_queue_size, 0),
        .stat_bufs = .empty,
        .outstanding_tasks = 0,

        .output = .empty,
    };
}
pub fn deinit(t: *IoUringTraverser) void {
    t.output.deinit(t.gpa);
    t.uring.deinit();
    var arena = t.arena.promote(t.gpa);
    arena.deinit();
}

pub fn run(t: *IoUringTraverser) !void {
    t.output.clearRetainingCapacity();

    try t.output.append(t.gpa, .{
        .parent = 0,
        .path = ".",
        .incomplete_children = undefined,
    });

    _ = try t.uring.nop(@bitCast(TaskData{
        .id = 0,
        .kind = .begin_dir,
    }));
    t.outstanding_tasks += try t.uring.submit();

    while (t.outstanding_tasks > 0) {
        var cqes: [64]linux.io_uring_cqe = undefined;
        const count = try t.uring.copy_cqes(&cqes, t.outstanding_tasks);
        t.outstanding_tasks -= count;

        for (cqes[0..count]) |cqe| {
            const data: TaskData = @bitCast(cqe.user_data);
            switch (data.kind) {
                .nop => {},

                .begin_dir => try t.processDir(data.id),
                .callback_dir => try t.processDirPart2(data, cqe),
                .finalize_dir => if (data.id == 0) {
                    // The root is complete
                    break;
                } else {
                    try t.finishItem(data.id);
                },

                .begin_file => try t.processFile(data.id),
                .callback_file => try t.processFilePart2(data, cqe),
            }
        }
        t.outstanding_tasks += try t.uring.submit();
    }
}

const TaskData = packed struct(u64) {
    id: u32,
    kind: enum(u4) {
        /// Use this for CQEs that don't need handling, such as `close`.
        nop,

        begin_dir,
        callback_dir,
        finalize_dir,

        begin_file,
        callback_file,
    },
    stat_buf: u28 = 0,
};

fn processFile(t: *IoUringTraverser, id: u32) !void {
    const stat_buf = try t.stat_bufs.add(t.gpa);
    errdefer t.stat_bufs.del(stat_buf);

    const sqe = try t.uring.get_sqe();
    sqe.prep_statx(
        t.root.fd,
        t.output.items[id].path,
        linux.AT.SYMLINK_NOFOLLOW | linux.AT.STATX_DONT_SYNC,
        linux.STATX_SIZE,
        t.stat_bufs.get(stat_buf),
    );
    sqe.user_data = @bitCast(TaskData{
        .id = id,
        .kind = .callback_file,
        .stat_buf = stat_buf,
    });
}
fn processFilePart2(t: *IoUringTraverser, data: TaskData, cqe: linux.io_uring_cqe) !void {
    defer t.stat_bufs.del(data.stat_buf);

    switch (cqe.err()) {
        .SUCCESS => {},
        .ACCES => return error.AccessDenied,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => unreachable,
        .LOOP => unreachable,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOTDIR => return error.NotDir,
        else => |err| return std.posix.unexpectedErrno(err),
    }

    t.output.items[data.id].size = t.stat_bufs.get(data.stat_buf).size;
    try t.finishItem(data.id);
}

fn processDir(t: *IoUringTraverser, id: u32) !void {
    _ = try t.uring.openat(
        @bitCast(TaskData{ .id = id, .kind = .callback_dir }),
        t.root.fd,
        t.output.items[id].path,
        .{
            .ACCMODE = .RDONLY,
            .NOFOLLOW = true,
            .DIRECTORY = true,
            .CLOEXEC = true,
        },
        0,
    );
}
fn processDirPart2(t: *IoUringTraverser, data: TaskData, cqe: linux.io_uring_cqe) !void {
    switch (cqe.err()) {
        .SUCCESS => {},

        .FAULT => unreachable,
        .INVAL => return error.BadPathName,
        .BADF => unreachable,
        .ACCES => return error.AccessDenied,
        .FBIG, .OVERFLOW => unreachable, // can't happen for directories
        .ISDIR => unreachable, // we set O_DIRECTORY
        .LOOP => return error.SymLinkLoop,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NAMETOOLONG => return error.NameTooLong,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NODEV => return error.NoDevice,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOSPC => unreachable, // we didn't set O_CREAT
        .NOTDIR => return error.NotDir,
        .PERM => return error.AccessDenied,
        .EXIST => unreachable, // we didn't set O_CREAT
        .BUSY => return error.DeviceBusy,
        .OPNOTSUPP => unreachable, // locking folders is not supported
        .AGAIN => unreachable, // can't happen for directories
        .TXTBSY => unreachable, // can't happen for directories
        .NXIO => return error.NoDevice,
        else => |err| return std.posix.unexpectedErrno(err),
    }

    const fd: linux.fd_t = cqe.res;
    defer _ = t.uring.close(@bitCast(TaskData{ .id = data.id, .kind = .nop }), fd) catch {
        // async close failed, fallback to synchronous
        std.posix.close(fd);
    };

    var arena = t.arena.promote(t.gpa);
    defer t.arena = arena.state;

    const path = std.mem.span(t.output.items[data.id].path);

    var count: u31 = 0;
    var dir: std.fs.Dir = .{ .fd = fd };
    var it = dir.iterateAssumeFirstIteration();
    while (try it.next()) |entry| {
        // TODO: open more persistent FDs to avoid lengthy sub-paths
        const child_path = try std.fs.path.joinZ(arena.allocator(), &.{
            path,
            entry.name,
        });

        const child = t.output.items.len;
        try t.output.append(t.gpa, .{
            .parent = data.id,
            .path = child_path,
            .incomplete_children = undefined,
        });
        count += 1;

        _ = try t.uring.nop(@bitCast(TaskData{
            .id = @intCast(child),
            .kind = switch (entry.kind) {
                .directory => .begin_dir,
                else => .begin_file,
            },
        }));
    }

    // Set actual child count
    t.output.items[data.id].incomplete_children = count;
    if (count == 0) {
        // No children; finalize right away
        _ = try t.uring.nop(@bitCast(TaskData{
            .id = data.id,
            .kind = .finalize_dir,
        }));
    }
}

fn finishItem(t: *IoUringTraverser, id: u32) !void {
    const result = &t.output.items[id];

    const parent_result = &t.output.items[result.parent];
    parent_result.size += result.size;

    parent_result.incomplete_children -= 1;
    std.debug.assert(result.incomplete_children >= 0);
    if (parent_result.incomplete_children == 0) {
        // All children completed
        _ = try t.uring.nop(@bitCast(TaskData{
            .id = result.parent,
            .kind = .finalize_dir,
        }));
    }
}

pub const Result = struct {
    parent: u32,
    path: [*:0]const u8,
    size: u64 = 0,
    incomplete_children: u32,
};

const std = @import("std");
const linux = std.os.linux;
const Store = @import("store.zig").Store;
