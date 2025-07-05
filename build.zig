pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const tsan = b.option(bool, "tsan", "Enable thread sanitizer (default: in Debug)") orelse (optimize == .Debug);
    const tracy_dep = b.dependency("tracy", .{
        .target = target,
        .optimize = optimize,
        .enable = b.option(bool, "tracy", "Enable tracy profiling (default: false)") orelse false,
        .allocation = b.option(bool, "tracy-alloc", "Enable tracy allocation profiling (default: true)") orelse true,
        .sampling = b.option(bool, "tracy-sampling", "Enable tracy's sampling profiler (default: true)") orelse true,
        .wait = true,
    });

    const exe = b.addExecutable(.{
        .name = "duz",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.sanitize_thread = tsan;
    exe.root_module.addImport("tracy", tracy_dep.module("tracy"));
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    b.step("run", "Run the app").dependOn(&run_cmd.step);
}

const std = @import("std");
