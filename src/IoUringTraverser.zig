const IoUringTraverser = @This();

const uring_queue_size = 4096;

gpa: std.mem.Allocator,
root: std.fs.Dir,

uring: linux.IoUring,
stat_bufs: Store(u28, linux.Statx),
overflow: std.ArrayListUnmanaged(Task),
outstanding_tasks: u32,

arena: std.heap.ArenaAllocator.State,
output: std.ArrayListUnmanaged(Result),

pub fn init(general_purpose_alloc: std.mem.Allocator, root: std.fs.Dir) !IoUringTraverser {
    // Increase FD limit
    // TODO: gracefully handle running out of FDs
    {
        var limit = try std.posix.getrlimit(.NOFILE);
        if (limit.cur < limit.max) {
            std.log.debug("raising FD limit from {} to {}", .{ limit.cur, limit.max });
            limit.cur = limit.max;
            std.posix.setrlimit(.NOFILE, limit) catch |err| {
                std.log.warn("failed to raise file descriptor limit: {s}", .{@errorName(err)});
            };
        }
    }

    return .{
        .gpa = general_purpose_alloc,
        .root = root,

        // OPTIM: IOPOLL, SQPOLL
        .uring = try linux.IoUring.init(uring_queue_size, 0),
        .stat_bufs = .empty,
        .overflow = .empty,
        .outstanding_tasks = 0,

        .arena = .{},
        .output = .empty,
    };
}
pub fn deinit(t: *IoUringTraverser) void {
    t.uring.deinit();
    t.stat_bufs.deinit(t.gpa);
    t.overflow.deinit(t.gpa);

    t.output.deinit(t.gpa);
    var arena = t.arena.promote(t.gpa);
    arena.deinit();
}

pub fn run(t: *IoUringTraverser) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    t.output.clearRetainingCapacity();

    try t.output.append(t.gpa, .{
        .parent = 0,
        .path = ".",
        .state = .pack(.uninitialized_directory),
    });

    try t.schedule(.{ .process_dir = 0 });
    t.outstanding_tasks += try t.uring.submit();

    while (t.outstanding_tasks > 0) {
        {
            const tr_ = tracy.traceNamed(@src(), "cqe loop");
            defer tr_.end();

            var cqes: [64]linux.io_uring_cqe = undefined;
            const count = try t.uring.copy_cqes(&cqes, t.outstanding_tasks);
            t.outstanding_tasks -= count;

            for (cqes[0..count]) |cqe| {
                const data: Task.IoData = @bitCast(cqe.user_data);
                (switch (data.task) {
                    .process_file => t.processFile(data, cqe),
                    .process_dir => t.processDir(data, cqe),
                    .close_fd => {}, // No callback required
                }) catch |err| {
                    const result = &t.output.items[data.id];
                    result.state = .pack(.{ .errored = err });
                    t.finishItem(data.id);
                };
            }
        }

        {
            const tr_ = tracy.traceNamed(@src(), "sq submit");
            defer tr_.end();
            t.outstanding_tasks += try t.uring.submit();
        }

        {
            const tr_ = tracy.traceNamed(@src(), "flush overflow");
            defer tr_.end();

            // Push overflow tasks back onto the IO queue
            while (t.overflow.getLastOrNull()) |task| {
                t.scheduleImmediately(task) catch break;
                t.overflow.items.len -= 1;
            }
        }
    }
}

fn schedule(t: *IoUringTraverser, task: Task) !void {
    t.scheduleImmediately(task) catch {
        try t.overflow.append(t.gpa, task);
    };
}
fn scheduleImmediately(t: *IoUringTraverser, task: Task) !void {
    const tr = tracy.trace(@src());
    defer tr.end();
    tr.addText(@tagName(task));

    const iodata: Task.IoData = .{
        .task = task,
        .id = switch (task) {
            .process_file, .process_dir => |id| id,
            .close_fd => 0,
        },
        .stat_buf = switch (task) {
            .process_file => try t.stat_bufs.add(t.gpa),
            else => 0,
        },
    };
    errdefer switch (task) {
        .process_file => t.stat_bufs.del(iodata.stat_buf),
        else => {},
    };

    const sqe = try t.uring.get_sqe();
    errdefer comptime unreachable;

    switch (task) {
        .process_file => |id| sqe.prep_statx(
            t.root.fd,
            t.output.items[id].path,
            linux.AT.SYMLINK_NOFOLLOW | linux.AT.STATX_DONT_SYNC,
            linux.STATX_SIZE,
            t.stat_bufs.get(iodata.stat_buf),
        ),

        .process_dir => |id| sqe.prep_openat(
            t.root.fd,
            t.output.items[id].path,
            .{
                .ACCMODE = .RDONLY,
                .NOFOLLOW = true,
                .DIRECTORY = true,
                .CLOEXEC = true,
            },
            0,
        ),

        .close_fd => |fd| sqe.prep_close(fd),
    }

    sqe.user_data = @bitCast(iodata);
}

const Task = union(enum(u4)) {
    process_file: u32,
    process_dir: u32,
    close_fd: linux.fd_t,

    const IoData = packed struct(u64) {
        id: u32,
        task: @typeInfo(Task).@"union".tag_type.?,
        stat_buf: u28 = 0,
    };
};

fn processFile(t: *IoUringTraverser, data: Task.IoData, cqe: linux.io_uring_cqe) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    const result = &t.output.items[data.id];
    if (tracy.enable) {
        tr.addText(std.mem.span(result.path));
    }

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

    std.debug.assert(result.state.unpack() == .incomplete_file);
    result.size = t.stat_bufs.get(data.stat_buf).size;
    result.state = .pack(.completed_file);
    t.finishItem(data.id);
}

fn processDir(t: *IoUringTraverser, data: Task.IoData, cqe: linux.io_uring_cqe) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    if (tracy.enable) {
        const result = &t.output.items[data.id];
        tr.addText(std.mem.span(result.path));
    }

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
    defer t.scheduleImmediately(.{ .close_fd = fd }) catch {
        // async close failed, fallback to synchronous
        std.posix.close(fd);
    };

    var arena = t.arena.promote(t.gpa);
    defer t.arena = arena.state;

    const path = std.mem.span(t.output.items[data.id].path);

    var count: u31 = 0;
    var dir: std.fs.Dir = .{ .fd = fd };
    var it = dir.iterateAssumeFirstIteration();
    while (true) {
        const tr_ = tracy.traceNamed(@src(), "iterate directory");
        defer tr_.end();

        const entry = blk: {
            const tr_next = tracy.traceNamed(@src(), "next");
            defer tr_next.end();
            break :blk try it.next();
        } orelse break;
        tr_.setName(entry.name);

        // TODO: open more persistent FDs to avoid lengthy sub-paths
        const child_path = try std.fs.path.joinZ(arena.allocator(), &.{ path, entry.name });

        const child: u32 = @intCast(t.output.items.len);
        try t.output.append(t.gpa, .{
            .parent = data.id,
            .path = child_path,
            .state = .pack(switch (entry.kind) {
                .directory => .uninitialized_directory,
                else => .incomplete_file,
            }),
        });

        if (t.schedule(switch (entry.kind) {
            .directory => .{ .process_dir = child },
            else => .{ .process_file = child },
        })) |_| {
            count += 1;
        } else |err| {
            const result = &t.output.items[child];
            result.state = .pack(.{ .errored = err });
        }
    }

    // Set actual child count
    t.output.items[data.id].state = .pack(.{ .incomplete_directory = count });
    if (count == 0) {
        // No children; finalize right away
        t.finishItem(data.id);
    }
}

fn finishItem(t: *IoUringTraverser, id_: u32) void {
    var id = id_;
    while (id != 0) {
        const tr = tracy.trace(@src());
        defer tr.end();

        const result = &t.output.items[id];
        if (tracy.enable) {
            tr.addText(std.mem.span(result.path));
        }

        const parent_result = &t.output.items[result.parent];
        parent_result.size += result.size;

        if (parent_result.state.directory.not_a_directory) {
            std.log.warn("{s} -> {s} {}", .{ result.path, parent_result.path, parent_result.state.unpack() });
        } else {
            parent_result.state.finishChildren(1);
        }
        if (parent_result.state.unpack() == .completed_directory) {
            // All children completed
            id = result.parent;
        } else {
            break;
        }
    }
}

const std = @import("std");
const tracy = @import("tracy");
const linux = std.os.linux;
const Store = @import("store.zig").Store;
const Result = @import("Result.zig");
