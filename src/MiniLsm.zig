const std = @import("std");
const storage = @import("storage.zig");

const Self = @This();

allocator: std.mem.Allocator,
inner: storage.StorageInner,
compaction_stopper: storage.Stopper,

pub fn init(allocator: std.mem.Allocator, path: []const u8, options: storage.StorageOptions) !Self {
    var inner = try storage.StorageInner.init(allocator, path, options);
    errdefer inner.deinit();

    var stopper = try inner.spawnCompactionThread();
    errdefer stopper.deinit();

    return .{
        .allocator = allocator,
        .inner = inner,
        .compaction_stopper = stopper,
    };
}

pub fn deinit(self: *Self) void {
    self.compaction_stopper.send(struct {}{});
    self.compaction_stopper.deinit();
    self.inner.deinit();
}
