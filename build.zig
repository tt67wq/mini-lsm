const std = @import("std");
const LazyPath = std.Build.LazyPath;

const c_source_files = [_][]const u8{"c_src/swal.c"};
const c_flags = [_][]const u8{};

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    const mini_lsm = b.addSharedLibrary(.{
        .name = "mini_lsm",
        .root_source_file = b.path("src/MiniLsm.zig"),
        .target = target,
        .optimize = optimize,
    });
    mini_lsm.linkLibC();
    mini_lsm.addIncludePath(LazyPath{ .cwd_relative = "./include" });
    mini_lsm.addCSourceFiles(.{
        .files = &c_source_files,
        .flags = &c_flags,
    });
    b.installArtifact(mini_lsm);

    const exe = b.addExecutable(.{
        .name = "demo",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.linkLibC();
    exe.addIncludePath(LazyPath{ .cwd_relative = "./include" });
    exe.addCSourceFiles(.{
        .files = &c_source_files,
        .flags = &c_flags,
    });

    if (b.option(bool, "enable-demo", "install demo app") orelse false) {
        b.installArtifact(exe);
    }

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const mini_lsm_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/MiniLsm.zig"),
        .target = target,
        .optimize = optimize,
    });
    mini_lsm_unit_tests.linkLibC();
    mini_lsm_unit_tests.addIncludePath(LazyPath{ .cwd_relative = "./include" });
    mini_lsm_unit_tests.addCSourceFiles(.{
        .files = &c_source_files,
        .flags = &c_flags,
    });

    const run_mini_lsm_unit_tests = b.addRunArtifact(mini_lsm_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_mini_lsm_unit_tests.step);
}
