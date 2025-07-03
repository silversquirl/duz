pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const tsan = b.option(bool, "tsan", "Enable thread sanitizer") orelse (optimize == .Debug);

    const exe = b.addExecutable(.{
        .name = "duz",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.sanitize_thread = tsan;
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    b.step("run", "Run the app").dependOn(&run_cmd.step);
}

const std = @import("std");
