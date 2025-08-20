pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const tsan = b.option(bool, "tsan", "Enable thread sanitizer (default: in Debug)") orelse (optimize == .Debug);

    const tracy_enable = b.option(bool, "tracy", "Enable tracy profiling (default: false)") orelse false;
    if (tracy_enable and optimize == .Debug) {
        std.log.warn("tracy is enabled, but compiling in debug mode", .{});
    }

    const tracy_dep = b.dependency("tracy", .{
        .target = target,
        .optimize = optimize,
        .enable = tracy_enable,
        .allocation = b.option(bool, "tracy-alloc", "Enable tracy allocation profiling (default: true)") orelse true,
        .sampling = b.option(bool, "tracy-sampling", "Enable tracy's sampling profiler (default: true)") orelse true,
        .wait = b.option(bool, "tracy-wait", "Wait for server to attach before exiting (default: true)") orelse true,
    });

    const opts = b.addOptions();
    const TraceLevel = enum { normal, verbose };
    const trace_level = b.option(TraceLevel, "trace-level", "Set the trace level (only applies when tracy is enabled)") orelse .normal;
    opts.addOption(TraceLevel, "trace_level", trace_level);

    const mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.sanitize_thread = tsan;
    mod.addImport("tracy", tracy_dep.module("tracy"));
    mod.addImport("tracy_always_disabled", tracy_dep.module("tracy_always_disabled"));
    mod.addImport("build_options", opts.createModule());

    const exe = b.addExecutable(.{
        .name = "duz",
        .root_module = mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    b.step("run", "Run the app").dependOn(&run_cmd.step);
}

const std = @import("std");
