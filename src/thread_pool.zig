/// `Worker` stores state that is specific to each worker thread. It must have three functions:
/// - `init(*Worker, ...) !void`, which runs on the main thread during `init`
/// - `deinit(*Worker) void`, which runs on the main thread during `deinit`
/// - `run(*Worker, Task) error{Canceled}!void`, which runs on the worker thread and is passed tasks sent to the pool
///
pub fn ThreadPool(comptime Worker: type, comptime Task: type) type {
    return struct {
        /// Shared state to be Futex-waited on
        wait_state: std.atomic.Value(packed struct(u32) {
            /// Set when the pool is canceled
            canceled: bool = false,
            /// Incremented whenever a queue becomes non-empty
            timeline: u31 = 0,
        }),
        /// The next runner to push tasks to
        round_robin: std.atomic.Value(usize),
        runners: []Runner,

        const Pool = @This();
        pub const empty: Pool = .{
            .wait_state = .init(.{}),
            .round_robin = .init(0),
            .queue_len = .init(0),
            .queues = .{},
        };

        threadlocal var thread_local_parent_pool: ?*Pool = null;
        threadlocal var thread_local_runner: *Runner = undefined; // valid iff thread_local_parent_pool != null

        pub fn init(pool: *Pool, gpa: std.mem.Allocator, thread_count_or_null: ?usize, worker_init_args: anytype) !void {
            const thread_count = thread_count_or_null orelse try std.Thread.getCpuCount();
            const runners = try gpa.alloc(Runner, thread_count);
            errdefer gpa.free(runners);

            pool.* = .{
                .wait_state = .init(.{}),
                .round_robin = .init(0),
                .runners = runners,
            };

            // Initialization happens in three phases. Thread init happens last, to ensure a
            // worker's `run` function does not race with initialization of other runners or workers.
            // Worker init happens after general runner state init, to ensure tasks can be added to
            // the pool from within a worker's `init` function, if desired.

            var runner_idx: usize = 0;
            errdefer for (runners[0..runner_idx]) |*runner| {
                gpa.free(runner.buf);
            };
            while (runner_idx < runners.len) : (runner_idx += 1) {
                const runner = &runners[runner_idx];
                runner.* = .{
                    .mutex = .{},
                    .buf = try gpa.alloc(Task, 32),
                    .front = 0,
                    .len = 0,
                    .canceled = false,

                    .worker = undefined,
                    .thread = undefined,

                    .plot_name_buf = undefined,
                    .plot = undefined,
                };
                errdefer gpa.free(runner.buf);

                if (tracy.enable) {
                    const queue_name = std.fmt.bufPrintZ(&runner.plot_name_buf, "worker {}", .{runner_idx}) catch "worker";
                    runner.plot = tracy.plot(queue_name);
                    runner.plot.update(0);
                }
            }

            // Initialize worker state
            var worker_idx: usize = 0;
            errdefer for (runners[0..worker_idx]) |*runner| {
                runner.worker.deinit();
            };
            while (worker_idx < runners.len) : (worker_idx += 1) {
                const runner = &runners[worker_idx];
                try @call(.auto, Worker.init, .{&runner.worker} ++ worker_init_args);
            }

            // Spawn threads
            var thread_idx: usize = 0;
            errdefer {
                _ = pool.wait_state.fetchOr(.{ .canceled = true }, .monotonic);
                for (runners[0..thread_idx]) |*runner| {
                    runner.cancel();
                }
                std.Thread.Futex.wake(@ptrCast(&pool.wait_state), std.math.maxInt(u32));
                for (runners[0..thread_idx]) |*runner| {
                    runner.thread.join();
                }
            }
            while (thread_idx < runners.len) : (thread_idx += 1) {
                const runner = &runners[thread_idx];
                runner.thread = try .spawn(.{}, Runner.main, .{ runner, pool });
            }
        }

        pub fn deinit(pool: *Pool, gpa: std.mem.Allocator) void {
            pool.cancel();
            for (pool.runners) |*runner| {
                runner.deinit(gpa);
            }
            gpa.free(pool.runners);
        }
        pub fn cancel(pool: *Pool) void {
            std.log.debug("cancel", .{});
            if (pool.wait_state.fetchOr(.{ .canceled = true }, .monotonic).canceled) {
                // Already canceled
                return;
            }
            for (pool.runners) |*runner| {
                runner.cancel();
            }
            std.Thread.Futex.wake(@ptrCast(&pool.wait_state), std.math.maxInt(u32));
        }

        pub fn waitForCancel(pool: *Pool) void {
            while (true) {
                const state = pool.wait_state.load(.monotonic);
                if (state.canceled) return;
                std.Thread.Futex.wait(@ptrCast(&pool.wait_state), @bitCast(state));
            }
        }

        fn bumpTimeline(pool: *Pool) void {
            _ = pool.wait_state.fetchAdd(.{ .timeline = 1 }, .monotonic);
            // TODO: may be possible to wake only one thread, as long as that thread then wakes more if it doesn't get what it needed
            std.Thread.Futex.wake(@ptrCast(&pool.wait_state), std.math.maxInt(u32));
        }

        pub fn run(pool: *Pool, gpa: std.mem.Allocator, task: Task) !void {
            if (thread_local_parent_pool == pool) {
                // Directly use the current thread's queue if we're in a worker thread
                const runner = thread_local_runner;
                if (runner.canceled) return error.Canceled;
                try runner.queueTask(gpa, pool, task);
            } else {
                // If we're not in a worker thread, use round-robin scheduling
                const state = pool.wait_state.load(.monotonic);
                if (state.canceled) return error.Canceled;

                var idx = pool.round_robin.fetchAdd(1, .monotonic);
                const expect = idx + 1;
                while (idx >= pool.runners.len) {
                    idx -= pool.runners.len;
                }
                if (idx + 1 != expect) {
                    // If this fails, the next call to `run` should succeed
                    _ = pool.round_robin.cmpxchgWeak(expect, idx + 1, .monotonic, .monotonic);
                }

                const runner = &pool.runners[idx];
                try runner.queueTask(gpa, pool, task);
            }
        }

        /// A runner receives tasks from the pool and dispatches them to the Worker
        const Runner = struct {
            // TODO: use double-checked locking for faster length checks
            // TODO: lock-free? :3
            mutex: std.Thread.Mutex align(std.atomic.cache_line),
            buf: []Task,
            front: u32,
            len: u32,
            canceled: bool,

            worker: Worker,
            thread: std.Thread,

            plot_name_buf: if (tracy.enable) [64]u8 else void,
            plot: tracy.Plot,

            fn deinit(runner: *Runner, gpa: std.mem.Allocator) void {
                runner.thread.join();
                runner.worker.deinit();
                gpa.free(runner.buf);
            }

            fn cancel(runner: *Runner) void {
                runner.mutex.lock();
                defer runner.mutex.unlock();
                runner.len = 0;
                runner.front = 0;
                runner.canceled = true;
            }

            fn main(runner: *Runner, pool: *Pool) void {
                thread_local_parent_pool = pool;
                thread_local_runner = runner;
                while (true) {
                    runner.tick(pool) catch return;
                }
            }
            fn tick(runner: *Runner, pool: *Pool) error{Canceled}!void {
                const tr = tracy.trace(@src());
                defer tr.end();
                const task = try runner.getTask(pool);
                try runner.worker.run(task);
            }

            fn queueTask(runner: *Runner, gpa: std.mem.Allocator, pool: *Pool, item: Task) error{ Canceled, OutOfMemory }!void {
                const tr = tracy.trace(@src());
                defer tr.end();
                tr.setColor(0xffaa00);

                runner.mutex.lock();
                defer runner.mutex.unlock();
                if (runner.canceled) return error.Canceled;

                if (runner.len >= runner.buf.len) {
                    // Alloc new buffer
                    const new_buf = try gpa.alloc(Task, runner.buf.len * 2);
                    errdefer comptime unreachable;

                    // Rebase into start of new buffer
                    const suffix_len = runner.buf.len - runner.front;
                    @memcpy(new_buf[0..suffix_len], runner.buf[runner.front..]);
                    if (runner.front != 0) {
                        @memcpy(new_buf[suffix_len..runner.buf.len], runner.buf[0..runner.front]);
                    }

                    gpa.free(runner.buf);
                    runner.buf = new_buf;
                    runner.front = 0;
                }

                runner.buf[runner.back()] = item;

                if (runner.len == 0) {
                    // No longer empty
                    pool.bumpTimeline();
                }
                runner.len += 1;
                runner.plot.update(runner.len);
            }

            fn getTask(runner: *Runner, pool: *Pool) error{Canceled}!Task {
                const tr = tracy.trace(@src());
                defer tr.end();
                tr.setColor(0xffaa00);

                // Try to pop from local queue
                if (runner.mutex.tryLock()) {
                    defer runner.mutex.unlock();
                    if (runner.canceled) return error.Canceled;

                    if (runner.len > 0) {
                        const task = runner.buf[runner.front];

                        runner.front += 1;
                        if (runner.front >= runner.buf.len) {
                            runner.front -= @intCast(runner.buf.len); // Wrap
                        }
                        runner.len -= 1;
                        runner.plot.update(runner.len);

                        return task;
                    }
                }

                // Steal from other runners
                while (true) {
                    const state = pool.wait_state.load(.monotonic);
                    if (state.canceled) return error.Canceled;

                    for (pool.runners) |*other| {
                        other.mutex.lock();
                        defer other.mutex.unlock();
                        if (runner.canceled) return error.Canceled;

                        if (other.len > 0) {
                            other.len -= 1;
                            other.plot.update(other.len);

                            return other.buf[other.back()];
                        }
                    }

                    // Wait for update
                    tracy.messageColor("block", 0xff0000);
                    std.Thread.Futex.wait(@ptrCast(&pool.wait_state), @bitCast(state));
                }
            }

            fn back(runner: *const Runner) u32 {
                const mask: u32 = @intCast(runner.buf.len - 1);
                return (runner.front + runner.len) & mask;
            }
        };
    };
}

const std = @import("std");
const tracy = @import("tracy");
