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

    const storage_lib = b.addSharedLibrary(.{
        .name = "storage",
        .root_source_file = b.path("src/storage.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_lib.linkLibC();
    storage_lib.addIncludePath(LazyPath{ .cwd_relative = "./include" });
    storage_lib.addCSourceFiles(.{
        .files = &c_source_files,
        .flags = &c_flags,
    });
    b.installArtifact(storage_lib);

    const exe = b.addExecutable(.{
        .name = "demo",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.linkLibrary(storage_lib);

    if (b.option(bool, "enable-demo", "install demo app") orelse false) {
        b.installArtifact(exe);
    }

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const storage_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/storage.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_unit_tests.linkLibC();
    storage_unit_tests.addIncludePath(LazyPath{ .cwd_relative = "./include" });
    storage_unit_tests.addCSourceFiles(.{
        .files = &c_source_files,
        .flags = &c_flags,
    });

    const run_storage_unit_tests = b.addRunArtifact(storage_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_storage_unit_tests.step);
}
