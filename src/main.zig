pub fn main() !void {
    defer if (root_allocator.is_debug) {
        _ = root_allocator.debug.deinit();
    };
    const gpa = root_allocator.allocator;

    const opts = try parseArgs(gpa);
    defer opts.arena.deinit();

    for (opts.paths) |path| {
        const tr = tracy.traceNamed(@src(), "path loop");
        defer tr.end();
        tr.setName(path);

        const dir = try std.fs.cwd().openDir(path, .{});

        var traverser: Traverser = switch (opts.backend) {
            inline else => |b| @unionInit(Traverser, @tagName(b), try .init(gpa, dir)),
        };
        defer switch (traverser) {
            inline else => |*t| t.deinit(),
        };
        switch (traverser) {
            .io_uring => |*t| try t.run(),
            .threaded => |*t| {
                const thread_count = opts.thread_count orelse try std.Thread.getCpuCount();
                try t.start(thread_count);
                t.join();
            },
        }

        const stdout = std.io.getStdOut().writer();
        var bufw = std.io.bufferedWriter(stdout);
        const out = bufw.writer();

        const count = switch (traverser) {
            .io_uring => |*t| t.output.items.len,
            .threaded => |*t| t.output.len.load(.acquire),
        };
        for (0..count) |id| {
            const result: Result = switch (traverser) {
                .io_uring => |*t| t.output.items[id],
                .threaded => |*t| t.output.getPtr(id).?.load(),
            };

            var suffix: []const u8 = "";
            switch (result.state.unpack()) {
                .completed_directory => {
                    suffix = std.fs.path.sep_str;
                },
                .completed_file => {},

                .errored => |err| {
                    std.log.err("{s}: {s}", .{ result.path, @errorName(err) });
                    continue;
                },

                .incomplete_directory => unreachable,
                .incomplete_file => unreachable,
            }

            out.print("{: >10.1}  {s}{s}\n", .{
                std.fmt.fmtIntSizeBin(result.size),
                result.path,
                suffix,
            }) catch |err| switch (err) {
                error.BrokenPipe => break,
                else => |e| return e,
            };
        }

        bufw.flush() catch |err| switch (err) {
            error.BrokenPipe => {},
            else => |e| return e,
        };
    }
}

const Traverser = union(Options.Backend) {
    io_uring: IoUringTraverser,
    threaded: ThreadedTraverser,
};

fn parseArgs(gpa: std.mem.Allocator) !Options {
    var opts: Options = .{
        .arena = .init(gpa),
    };
    errdefer opts.arena.deinit();

    var paths: std.ArrayListUnmanaged([]const u8) = .{};
    defer paths.deinit(gpa);

    var args = try std.process.argsWithAllocator(opts.arena.allocator());
    _ = args.skip();

    while (args.next()) |flag| {
        if (std.mem.eql(u8, flag, "--")) {
            break;
        } else if (std.mem.eql(u8, flag, "-h") or std.mem.eql(u8, flag, "--help")) {
            std.io.getStdOut().writeAll(help) catch {};
            std.process.exit(0);
        } else if (std.mem.eql(u8, flag, "--backend")) {
            const arg = args.next() orelse {
                std.log.err("missing {s} argument", .{flag});
                std.process.exit(1);
            };
            if (std.meta.stringToEnum(Options.Backend, arg)) |backend| {
                opts.backend = backend;
            }
        } else if (std.mem.eql(u8, flag, "-j") or std.mem.eql(u8, flag, "--threads")) {
            const arg = args.next() orelse {
                std.log.err("missing {s} argument", .{flag});
                std.process.exit(1);
            };
            opts.thread_count = try std.fmt.parseUnsigned(usize, arg, 10);
        } else if (std.mem.startsWith(u8, flag, "-")) {
            std.log.err("unknown flag '{s}'", .{flag});
            std.process.exit(1);
        } else {
            try paths.append(gpa, flag);
        }
    }

    while (args.next()) |arg| try paths.append(gpa, arg);

    if (paths.items.len > 0) {
        opts.paths = try opts.arena.allocator().dupe([]const u8, paths.items);
    }

    return opts;
}

const Options = struct {
    arena: std.heap.ArenaAllocator,
    backend: Backend = .io_uring,
    thread_count: ?usize = null,
    paths: []const []const u8 = &.{"."},

    const Backend = enum { io_uring, threaded };
};

const usage = "Usage: duz [options] [paths...]\n";
const help = usage ++
    \\
    \\Options:
    \\  -h, --help              Print this help message and exit
    \\  --backend               Select backend to use ([io_uring], threaded)
    \\  -j <n>, --threads <n>   Number of threads to use for threaded backend
    \\
    \\
;

const root_allocator = struct {
    const is_debug = @import("builtin").mode == .Debug;
    var debug: if (is_debug) std.heap.DebugAllocator(.{}) = .init;

    const impl = if (is_debug) debug.allocator() else std.heap.smp_allocator;
    var traced: if (tracy.enable_allocation) tracy.TracyAllocator("root") = .init(impl);
    const allocator = if (tracy.enable_allocation) traced.allocator() else impl;
};

const std = @import("std");
const tracy = @import("tracy");
const Result = @import("Result.zig");
const ThreadedTraverser = @import("Traverser.zig");
const IoUringTraverser = @import("IoUringTraverser.zig");
