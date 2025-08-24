pub fn main() !void {
    const tr = tracy.trace(@src());
    defer tr.end();

    defer if (root_allocator.is_debug) {
        _ = root_allocator.debug.deinit();
    };
    const gpa = root_allocator.allocator;

    const opts = try parseArgs(gpa);
    defer opts.arena.deinit();
    const thread_count = opts.thread_count orelse (try std.Thread.getCpuCount()) * 5 / 2;

    var out_buf: [4096]u8 = undefined;
    var out = std.fs.File.stdout().writerStreaming(&out_buf);

    for (opts.paths) |path| {
        const tr_loop = tracy.traceNamed(@src(), "path loop iteration");
        defer tr_loop.end();
        tr_loop.setName(path);

        const dir = try std.fs.cwd().openDir(path, .{});

        var traverser: Traverser = try .init(gpa, dir);
        defer traverser.deinit();
        try traverser.start(thread_count);
        traverser.join();

        const tr_print = tracy.traceNamed(@src(), "print results");
        defer tr_print.end();
        const count = traverser.output.len.load(.acquire);
        std.log.debug("{} results", .{count});
        for (0..count) |id| {
            const result: Result = traverser.output.getPtr(id).?.load();

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

            out.interface.print("{Bi: >10.1}  {s}{s}\n", .{
                result.size,
                result.path,
                suffix,
            }) catch switch (out.err orelse error.WriteFailed) {
                error.BrokenPipe => break,
                else => |e| return e,
            };
        }

        out.interface.flush() catch switch (out.err orelse error.WriteFailed) {
            error.BrokenPipe => break,
            else => |e| return e,
        };
    }
}

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
            std.fs.File.stdout().writeAll(help) catch {};
            std.process.exit(0);
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
    thread_count: ?usize = null,
    paths: []const []const u8 = &.{"."},
};

const usage = "Usage: duz [options] [paths...]\n";
const help = usage ++
    \\
    \\Options:
    \\  -h, --help              Print this help message and exit
    \\  -j <n>, --threads <n>   Number of threads to use
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
const Traverser = @import("Traverser.zig");
