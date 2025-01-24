const std = @import("std");
const storage = @import("storage.zig");

const Self = @This();

allocator: std.mem.Allocator,
inner: storage.StorageInner,

pub fn init(allocator: std.mem.Allocator, path: []const u8, options: storage.StorageOptions) !Self {
    var inner = try storage.StorageInner.init(allocator, path, options);
    errdefer inner.deinit();

    inner.spawnCompactionThread();

    return .{
        .allocator = allocator,
        .inner = inner,
    };
}

pub fn deinit(self: *Self) void {
    self.inner.syncDir() catch |err| {
        std.log.err("failed to sync dir: {s}", .{@errorName(err)});
    };
    self.inner.deinit();
}
