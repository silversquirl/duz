pub fn main() !void {
    var args = std.process.args();
    _ = args.skip();
    const thread_count = if (args.next()) |arg|
        try std.fmt.parseUnsigned(usize, arg, 10)
    else
        try std.Thread.getCpuCount();

    var t: Traverser = try .init(std.heap.smp_allocator, std.fs.cwd());
    defer t.deinit();

    try t.start(thread_count);
    t.join();

    const stdout = std.io.getStdOut().writer();
    var bufw = std.io.bufferedWriter(stdout);
    const out = bufw.writer();

    for (0..t.output.len.load(.acquire)) |id| {
        const result = t.output.getPtr(id).?;
        try out.print("{:.2}\t'{s}'\n", .{
            std.fmt.fmtIntSizeBin(result.size.load(.acquire)),
            result.path,
        });
    }

    try bufw.flush();
}

const std = @import("std");
const Traverser = @import("Traverser.zig");
