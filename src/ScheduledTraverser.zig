const ScheduledTraverser = @This();

const max_workers = 64;

gpa: std.mem.Allocator,
worker_count: usize,

workers: [max_workers]Worker,
available_fds: u64,

nodes: std.SegmentedList(Node, 128),
leaves: std.ArrayListUnmanaged(*Node),
files: std.ArrayListUnmanaged(*Node),

pub fn run(
    gpa: std.mem.Allocator,
    root: std.fs.Dir,
    worker_count: usize,
) !struct { []Result, std.heap.ArenaAllocator } {
    const tr = tracy_verbose.trace(@src());
    defer tr.end();
    tr.setColor(0x666666);

    // Max out FD limit
    var fd_limit = try std.posix.getrlimit(.NOFILE);
    if (fd_limit.cur < fd_limit.max) {
        std.log.debug("raising FD limit from {} to {}", .{ fd_limit.cur, fd_limit.max });
        fd_limit.cur = fd_limit.max;
        std.posix.setrlimit(.NOFILE, fd_limit) catch |err| {
            // Should never happen, unless someone is messing with us
            std.debug.panic("failed to raise file descriptor limit: {s}", .{@errorName(err)});
        };
    }

    if (worker_count > max_workers) {
        std.log.warn("worker thread count capped at {}", .{max_workers});
    }

    var t: ScheduledTraverser = .{
        .gpa = gpa,
        .worker_count = @max(1, @min(worker_count, max_workers)),

        .workers = undefined,
        .available_fds = fd_limit.cur,
        .nodes = .{},
        .leaves = .empty,
        .files = .empty,
    };
    defer t.deinit();

    try t.initPool();
    // do this at the function level, so the pool arenas are still alive when we build the result list
    defer t.deinitPool();

    { // Run traversal
        const root_node = try t.nodes.addOne(t.gpa);
        root_node.* = .{
            .fd = try Node.realOpenDir(root.fd, "."),
            .name = ".", // TODO: use the actual path we're searching
            .parent = null,
            .incomplete_children = undefined,
        };
        defer {
            const tr_ = tracy.traceNamed(@src(), "close directories");
            defer tr_.end();
            var it = t.nodes.iterator(0);
            while (it.next()) |node| node.closeDir();
        }

        try t.leaves.append(t.gpa, root_node);

        try t.runLoop();
    }

    // Collect results
    var arena: std.heap.ArenaAllocator = .init(gpa);
    errdefer arena.deinit();

    const results = try arena.allocator().alloc(Result, t.nodes.len);

    var node_iter = t.nodes.iterator(0);
    while (node_iter.next()) |node| {
        const i = node_iter.index - 1;

        const is_file = node.incomplete_children == Node.is_file_sentinel;
        if (!is_file) std.debug.assert(node.incomplete_children == 0);
        // Steal the incomplete_children field for nefarious purposes (storing the result index)
        node.incomplete_children = i;

        // Get parent result
        const parent_idx = if (node.parent) |p| p.incomplete_children else 0;

        const path = if (node.parent != null)
            try std.fs.path.joinZ(arena.allocator(), &.{
                std.mem.span(results[parent_idx].path),
                std.mem.span(node.name),
            })
        else
            try arena.allocator().dupeZ(u8, std.mem.span(node.name));

        results[i] = .{
            .parent = std.math.cast(u32, parent_idx) orelse {
                return error.TooManyResults;
            },
            .path = path.ptr,
            .size = node.size,
            .state = .pack(if (is_file)
                .completed_file
            else
                .completed_directory),
        };
    }

    return .{ results, arena };
}
fn deinit(t: *ScheduledTraverser) void {
    const tr = tracy.trace(@src());
    defer tr.end();

    t.nodes.deinit(t.gpa);
    t.leaves.deinit(t.gpa);
    t.files.deinit(t.gpa);
    t.* = undefined;
}

fn initPool(t: *ScheduledTraverser) !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    for (t.workers[0..t.worker_count], 0..) |*w, i| {
        w.* = .{
            .thread = undefined,
            .task_plot = undefined,
        };
        const thread_name = std.fmt.bufPrintZ(&w.thread_name_buf, "worker {}", .{i}) catch &w.thread_name_buf;
        w.task_plot = .init(thread_name);
        w.thread = try std.Thread.spawn(.{}, Worker.main, .{ w, t.gpa });
        w.thread.setName(thread_name) catch {};
    }
}
fn deinitPool(t: *ScheduledTraverser) void {
    const tr = tracy.trace(@src());
    defer tr.end();

    for (t.workers[0..t.worker_count]) |*w| {
        _ = w.send(.exit);
    }
    for (t.workers[0..t.worker_count]) |*w| {
        w.thread.join();
        var arena = w.arena.promote(t.gpa);
        arena.deinit();
    }
}

fn runLoop(t: *ScheduledTraverser) !void {
    const tr = tracy.trace(@src());
    defer tr.end();
    tr.setColor(0x666666);

    const tr_plots = .{
        .nodes = tracy.plot("nodes"),
        .leaves = tracy.plot("leaves"),
        .files = tracy.plot("files"),
    };

    // TODO: better scheduling algorithm
    // - PDF https://doi.org/10.1145/301970.301974
    // - AsyncDF https://www.cs.cmu.edu/~guyb/papers/toplas98.pdf

    var worker: usize = 0;
    var queued_tasks: usize = 0;
    var state: SearchState = .more;
    while (state != .done) {
        std.debug.assert(queued_tasks != 0);

        const tr_ = tracy_verbose.traceNamed(@src(), "runLoop iteration");
        defer tr_.end();
        tr_.setColor(0x777777);

        tr_plots.nodes.update(t.nodes.len);
        tr_plots.leaves.update(t.leaves.items.len);
        tr_plots.files.update(t.files.items.len);

        // OPTIM: may be better to do leaves first and then files, so we can build up a bigger backlog of files
        const task: Task = if (t.files.pop()) |node| .{
            .file_size = node,
        } else if (t.leaves.pop()) |node| .{
            .list_dir = .{ .node = node },
        } else .none;

        const completed = t.workers[worker].send(task);
        worker = (worker + 1) % t.worker_count;

        queued_tasks += @intFromBool(task != .none);
        queued_tasks -= @intFromBool(completed != .none);

        switch (completed) {
            .none => {},
            .exit => unreachable,

            .file_size => |node| if (node.parent) |parent| {
                if (tracy_verbose.enable) {
                    var buf: [512:0]u8 = undefined;
                    const str = std.fmt.bufPrintZ(&buf, "completed file {s}", .{node.name}) catch &buf;
                    tracy.messageColorCopy(str, 0xaaffff);
                }
                parent.size += node.size;
                parent.incomplete_children -= 1;
                if (parent.incomplete_children == 0) {
                    @branchHint(.unlikely);
                    state = finalizeDir(parent);
                }
            },
            .list_dir => |params| {
                defer params.out.deinit(t.gpa);
                if (tracy.enable) {
                    var buf: [512:0]u8 = undefined;
                    const str = std.fmt.bufPrintZ(&buf, "completed dir {s}", .{params.node.name}) catch &buf;
                    tracy.messageColorCopy(str, 0xaaffff);
                }
                state = try t.ingestDir(params.node, params.out);
            },
        }
    }

    std.debug.assert(queued_tasks == 0);
}

fn ingestDir(t: *ScheduledTraverser, node: *Node, dir: fs.Listing) !SearchState {
    const tr = tracy.trace(@src());
    defer tr.end();

    try t.nodes.growCapacity(t.gpa, t.nodes.len + dir.entries.len);
    try t.leaves.ensureUnusedCapacity(t.gpa, dir.dir_count);
    try t.files.ensureUnusedCapacity(t.gpa, dir.entries.len - dir.dir_count);
    errdefer comptime unreachable;

    for (dir.entries) |entry| {
        // SegmentedList has no appendAssumeCapacity? fine, i'll do it myself :3
        const child = t.nodes.uncheckedAt(t.nodes.len);
        t.nodes.len += 1;
        child.* = .{
            .name = entry.name,
            .parent = node,
            .incomplete_children = switch (entry.kind) {
                .directory => undefined,
                else => Node.is_file_sentinel,
            },
        };

        switch (entry.kind) {
            .directory => t.leaves.appendAssumeCapacity(child),
            else => t.files.appendAssumeCapacity(child),
        }
    }

    node.incomplete_children = dir.entries.len;
    std.debug.assert(node.incomplete_children != Node.is_file_sentinel);
    if (node.incomplete_children > 0) {
        @branchHint(.likely);
        return .more;
    } else {
        return finalizeDir(node);
    }
}

fn finalizeDir(node: *Node) SearchState {
    const tr = tracy.trace(@src());
    defer tr.end();

    var node_ = node;
    while (node_.parent) |parent| : (node_ = parent) {
        parent.size += node.size;
        parent.incomplete_children -= 1;
        if (parent.incomplete_children > 0) {
            @branchHint(.likely);
            return .more;
        }
    } else {
        @branchHint(.unlikely);
        return .done;
    }
}
const SearchState = enum { more, done };

const Node = struct {
    // IMPORTANT: make sure the root node always has a valid fd
    fd: linux.fd_t = -1,
    name: [*:0]const u8,
    size: u64 = 0,
    parent: ?*Node,
    incomplete_children: usize,

    // If incomplete_children == is_file_sentinel, this is a file, not a directory
    const is_file_sentinel = std.math.maxInt(usize);

    fn openDir(node: *Node) !linux.fd_t {
        const tr = tracy.trace(@src());
        defer tr.end();

        if (node.fd < 0) {
            const parent_fd = try node.parent.?.openDir(); // TODO: avoid recursion
            node.fd = try node.reportFail("open directory", realOpenDir(parent_fd, node.name));
        }
        return node.fd;
    }
    fn closeDir(node: *Node) void {
        const tr = tracy.trace(@src());
        defer tr.end();

        if (node.fd >= 0) {
            std.posix.close(node.fd);
            node.fd = -1;
        }
    }

    fn realOpenDir(parent_fd: linux.fd_t, name: [*:0]const u8) !linux.fd_t {
        const tr = tracy.trace(@src());
        defer tr.end();
        return std.posix.openatZ(parent_fd, name, .{
            .ACCMODE = .RDONLY,
            .NOFOLLOW = true,
            .CLOEXEC = true,
            .DIRECTORY = true,
        }, 0);
    }

    fn reportFail(node: Node, operation: []const u8, result: anytype) @TypeOf(result) {
        _ = result catch |err| {
            std.log.err("Failed to {s} '{f}': {s}", .{ operation, node, @errorName(err) });
        };
        return result;
    }

    pub fn format(node: *const Node, writer: *std.Io.Writer) !void {
        var path: [128][*:0]const u8 = undefined;
        var path_start: usize = path.len;
        {
            var n = node;
            path_start -= 1;
            path[path_start] = n.name;
            while (n.parent) |parent| {
                if (path_start == 0) {
                    path[0] = "...";
                    break;
                }
                n = parent;
                path_start -= 1;
                path[path_start] = n.name;
            }
        }

        for (path[path_start..], 0..) |name, i| {
            if (i > 0) {
                try writer.writeByte('/');
            }
            try writer.writeAll(std.mem.span(name));
        }
    }
};

const Worker = struct {
    thread: std.Thread,
    arena: std.heap.ArenaAllocator.State = .{},
    thread_name_buf: [31:0]u8 = undefined,

    // single-reader, single-writer ring buffer
    // TODO: give each task its own cache line?
    // OPTIM: determine optimal capacity
    tasks: [4]Task align(std.atomic.cache_line) = undefined,
    write_head: std.atomic.Value(u32) align(std.atomic.cache_line) = .init(0),
    read_head: std.atomic.Value(u32) align(std.atomic.cache_line) = .init(0),

    task_plot: if (tracy.enable) struct {
        plot: tracy.Plot,
        count: u32,
        lock: std.Thread.Mutex,

        fn init(name: [*:0]const u8) @This() {
            const plot = tracy.plot(name);
            plot.config(.{ .step = true });
            return .{
                .plot = plot,
                .count = 0,
                .lock = .{},
            };
        }

        fn send(plot: *@This()) void {
            plot.lock.lock();
            defer plot.lock.unlock();
            plot.count += 1;
            plot.plot.update(plot.count);
        }

        fn consume(plot: *@This()) void {
            plot.lock.lock();
            defer plot.lock.unlock();
            plot.count -= 1;
            plot.plot.update(plot.count);
        }
    } else struct {
        fn init(_: []const u8) @This() {
            return .{};
        }
        fn send(_: @This()) void {}
        fn consume(_: @This()) void {}
    },

    comptime {
        // the capacity must be a power of two for two reasons:
        // - cheap modulo operation for wraparound
        // - evenly divides a u32, so we don't have to worry about resetting when we overflow
        std.debug.assert(std.math.isPowerOfTwo(@as(Worker, undefined).tasks.len));
    }

    fn main(w: *Worker, gpa: std.mem.Allocator) void {
        const tr = tracy.trace(@src());
        defer tr.end();
        tr.setColor(0x666666);
        tracy.setThreadName(&w.thread_name_buf);

        var arena = w.arena.promote(gpa);
        defer w.arena = arena.state;

        while (true) {
            const tr_ = tracy_verbose.traceNamed(@src(), "worker iteration");
            defer tr_.end();
            tr_.setColor(0x777777);

            const task = w.peek();
            defer w.consume();
            task.trace(tr_);

            task.run(gpa, arena.allocator()) catch |err| switch (err) {
                error.WorkerExit => break,
                else => |e| {
                    tracy.messageColorCopy(@errorName(e), 0xff0000);
                    // TODO: error handling
                    @panic(@errorName(e));
                },
            };
        }
    }

    /// Returns the next task in the queue, without consuming it.
    fn peek(w: *Worker) *Task {
        const tr = tracy.trace(@src());
        defer tr.end();
        tr.setColor(0xffaa00);

        while (true) {
            const read = w.read_head.raw; // don't need atomics; we're the only writer to this
            const write = w.write_head.load(.acquire);
            if (read == write) {
                // Queue is empty
                std.Thread.Futex.wait(&w.write_head, write);
            } else {
                return &w.tasks[read % w.tasks.len];
            }
        }
    }
    /// Consumes the next task in the queue.
    fn consume(w: *Worker) void {
        const tr = tracy.trace(@src());
        defer tr.end();

        w.task_plot.consume();
        w.read_head.store(w.read_head.raw +% 1, .release);
        std.Thread.Futex.wake(&w.read_head, 1);
    }

    /// Returns the task that was in the newly occupied slot, so its completion can be registered.
    fn send(w: *Worker, task: Task) Task {
        const tr = tracy.trace(@src());
        defer tr.end();
        if (tracy.enable) {
            tr.addText(std.mem.sliceTo(&w.thread_name_buf, 0));
        }
        task.trace(tr);

        while (true) {
            const write = w.write_head.raw; // don't need atomics; we're the only writer to this
            const read = w.read_head.load(.acquire);
            if (write != read and // not empty
                write % w.tasks.len == read % w.tasks.len) // referencing same slot
            {
                // Queue is full
                std.Thread.Futex.wait(&w.read_head, read);
            } else {
                const idx = write % w.tasks.len;
                const completed = w.tasks[idx];
                w.tasks[idx] = task;
                w.task_plot.send();
                w.write_head.store(write +% 1, .release);
                std.Thread.Futex.wake(&w.write_head, 1);
                return completed;
            }
        }
    }
};

const Task = union(enum) {
    none,
    file_size: *Node,
    list_dir: struct {
        node: *Node,
        out: fs.Listing = undefined,
    },
    exit,

    fn run(task: *Task, gpa: std.mem.Allocator, arena: std.mem.Allocator) !void {
        const tr = tracy.trace(@src());
        defer tr.end();
        task.trace(tr);

        switch (task.*) {
            .file_size => |node| {
                const parent = node.parent.?;
                const parent_fd = try parent.openDir();
                defer {} // leave the fd open so we can reuse it
                node.size = try node.reportFail("stat file", fs.fileSize(parent_fd, node.name));
                std.log.debug("file {f} is {Bi:.2}", .{ node, node.size });
            },

            .list_dir => |*params| {
                const fd = try params.node.openDir();
                defer {} // leave the fd open so we can reuse it
                params.out = try params.node.reportFail("read directory", fs.listDir(gpa, arena, fd));
                std.log.debug("dir {f} has {} children", .{ params.node, params.out.entries.len });
            },

            .none => {},
            .exit => return error.WorkerExit,
        }
    }

    fn trace(task: Task, tr: anytype) void {
        if (tracy.enable) {
            tr.addText(@tagName(task));
            switch (task) {
                .file_size => |node| tr.addText(std.mem.span(node.name)),
                .list_dir => |params| tr.addText(std.mem.span(params.node.name)),
                else => {},
            }
        }
    }
};

const std = @import("std");
const linux = std.os.linux;

const tracy = @import("tracy");
const trace_level = @import("build_options").trace_level;
const tracy_verbose = if (trace_level == .verbose) tracy else @import("tracy_always_disabled");

const fs = @import("fs.zig");
const Store = @import("store.zig").Store;
const Result = @import("Result.zig");
