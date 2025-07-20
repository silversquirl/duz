const Traverser = @This();

const max_threads = 64;

gpa: std.mem.Allocator,
root: std.fs.Dir,
thread_count: u16,

arenas: [max_threads]std.heap.ArenaAllocator.State,
threads: [max_threads]std.Thread,
completed_outputs: std.atomic.Value(u32),
output: ts.SegmentedList(Result.ThreadSafe, 1024),
queue: ts.Queue(u32),
joined: bool = false,

pub fn init(general_purpose_alloc: std.mem.Allocator, root: std.fs.Dir) !Traverser {
    return .{
        .gpa = general_purpose_alloc,
        .root = root,
        .thread_count = 0,

        .arenas = @splat(.{}),
        .threads = undefined,
        .completed_outputs = .init(0),
        .output = .empty,
        .queue = .init("traversal queue"),
    };
}

/// Start a traversal.
/// TODO: allow reusing a Traverser for multiple traversals
pub fn start(t: *Traverser, thread_count: usize) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    t.output.clearRetainingCapacity();
    t.queue.array.clearRetainingCapacity();

    const root_id = try t.output.append(t.gpa, .{
        .parent = 0,
        .path = ".",
        .state = .init(.uninitialized_directory),
    });
    std.debug.assert(root_id == 0);
    try t.queue.array.append(t.gpa, @intCast(root_id));

    t.thread_count = @max(1, @min(thread_count, max_threads));
    if (thread_count > max_threads) {
        std.log.warn("thread count capped at {}", .{max_threads});
    }

    for (t.threads[0..t.thread_count], 0..) |*thread, thread_id| {
        thread.* = try std.Thread.spawn(.{}, worker, .{ t, @as(u16, @intCast(thread_id)) });
    }
}

/// Stop the search, wait for threads to exit, and release all resources.
pub fn deinit(t: *Traverser) void {
    t.stop();
    t.join();

    t.output.deinit(t.gpa);
    t.queue.deinit(t.gpa);

    for (t.arenas[0..t.thread_count]) |state| {
        var arena = state.promote(t.gpa);
        arena.deinit();
    }
}

pub fn stop(t: *Traverser) void {
    t.queue.close();
}

/// Wait for all threads to exit. Not thread-safe.
pub fn join(t: *Traverser) void {
    const tr = tracy.trace(@src());
    defer tr.end();

    if (t.joined) return;
    t.joined = true;
    for (t.threads[0..t.thread_count]) |thread| {
        thread.join();
    }
}

/// Wait for updates to the result buffer.
/// Returns null if the traversal has finished.
pub fn poll(t: *Traverser, prev_completed_outputs: u32) ?u32 {
    const tr = tracy.trace(@src());
    defer tr.end();

    while (true) {
        const completed = t.completed_outputs.load(.monotonic);
        if (completed != prev_completed_outputs) return completed;

        // There is no race here despite the values being read at diifferent times, because:
        // - they are both strictly increasing
        // - `output.len` increases before `completed_outputs`
        // - if they are ever equal, the search has ended and they will not be updated again
        if (t.output.len.load(.monotonic) == completed) return null;

        std.Thread.Futex.wait(&t.completed_outputs, prev_completed_outputs);
    }
}

fn worker(t: *Traverser, thread_id: u16) void {
    const tr = tracy.trace(@src());
    defer tr.end();

    while (t.queue.pop()) |id| {
        t.process(thread_id, id) catch |err| {
            const path = t.output.getPtr(id).?.path;
            std.log.err("{s}: {s}", .{ path, @errorName(err) });
        };
    }
}

// TODO: use local stack buffer to reduce contention
fn process(t: *Traverser, thread_id: u16, id: u32) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    const result = t.output.getPtr(id).?;
    if (tracy.enable) {
        tr.setName(std.mem.span(result.path));
    }

    const state = result.state.load(.monotonic);
    switch (state.unpack()) {
        .incomplete_directory => |count| {
            std.debug.assert(count == Result.State.uninitialized_directory.incomplete_directory);
            try t.processDir(thread_id, id, result);
        },

        .incomplete_file => try t.processFile(id, result),

        .completed_directory => unreachable,
        .completed_file => unreachable,
        .errored => unreachable,
    }
}

fn processFile(t: *Traverser, id: u32, result: *Result.ThreadSafe) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    const linux = std.os.linux;
    var stx: linux.Statx = undefined;
    const rc = linux.statx(
        t.root.fd,
        result.path,
        linux.AT.SYMLINK_NOFOLLOW | linux.AT.STATX_DONT_SYNC,
        linux.STATX_SIZE,
        &stx,
    );
    switch (linux.E.init(rc)) {
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

    result.size.store(stx.size, .monotonic);
    result.state.store(.pack(.completed_file), .monotonic);
    t.finishItem(id, result, stx.size);
}

fn processDir(t: *Traverser, thread_id: u16, id: u32, result: *Result.ThreadSafe) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    var arena = t.arenas[thread_id].promote(t.gpa);
    defer t.arenas[thread_id] = arena.state;

    const path = std.mem.span(result.path);

    var dir = blk: {
        const tr_ = tracy.traceNamed(@src(), "openDir");
        defer tr_.end();

        break :blk try t.root.openDirZ(path, .{ .iterate = true, .no_follow = true });
    };
    defer dir.close();

    var count: u31 = 0;
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

        // TODO: open more FDs to avoid lengthy sub-paths
        const child_path = try std.fs.path.joinZ(arena.allocator(), &.{ path, entry.name });

        // OPTIM: test whether it's faster to `stat` files now, rather than queueing them
        const child = try t.output.append(t.gpa, .{
            .parent = id,
            .path = child_path,
            .state = .init(switch (entry.kind) {
                .directory => .uninitialized_directory,
                else => .incomplete_file,
            }),
        });
        count += 1;

        try t.queue.push(t.gpa, @intCast(child));
    }

    // Set actual child count. We do this by subtracting rather than storing, in case any children have already been completed.
    const delta = Result.State.uninitialized_directory.incomplete_directory - count;
    t.finishChildren(id, result, delta);
}

fn finishItem(t: *Traverser, id: u32, result: *Result.ThreadSafe, size: u64) void {
    // TODO: can't deal with tail calls
    // const tr = tracy.trace(@src());
    // defer tr.end();

    if (id == 0) {
        // The root is complete
        t.stop();
    }

    _ = t.completed_outputs.fetchAdd(1, .release);
    std.Thread.Futex.wake(&t.completed_outputs, std.math.maxInt(u32));

    if (id != 0) {
        // OPTIM: this is really bad, probably causes a ton of false sharing. batch updates for files in the same dir
        const parent_id = result.parent;
        const parent_result = t.output.getPtr(parent_id).?;
        _ = parent_result.size.fetchAdd(size, .monotonic);
        @call(.always_tail, finishChildren, .{ t, parent_id, parent_result, 1 });
    }
}

// Takes a u64 to match the signature of finishItem, for tail calling reasons
fn finishChildren(t: *Traverser, id: u32, result: *Result.ThreadSafe, count: u64) void {
    // TODO: can't deal with tail calls
    // const tr = tracy.trace(@src());
    // defer tr.end();

    const new = result.state.finishChildren(@intCast(count), .acq_rel).unpack();
    if (new == .completed_directory) {
        // All children completed
        // TODO: avoid recursion
        @call(.always_tail, finishItem, .{ t, id, result, result.size.load(.monotonic) });
    }
}

const std = @import("std");
const tracy = @import("tracy");
const ts = @import("ts.zig");
const Result = @import("Result.zig");
