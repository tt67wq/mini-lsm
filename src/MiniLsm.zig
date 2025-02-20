const std = @import("std");
const storage = @import("storage.zig");
const LsmIterator = @import("iterators.zig").LsmIterator;

pub const WriteBatchRecord = storage.WriteBatchRecord;
pub const StorageOptions = storage.StorageOptions;
pub const Bound = @import("MemTable.zig").Bound;

const Self = @This();

allocator: std.mem.Allocator,
inner: storage.StorageInner,

pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) !Self {
    var inner = try storage.StorageInner.init(allocator, path, options);
    errdefer inner.deinit();

    try inner.syncDir();

    inner.spawnFlushThread();
    inner.spawnCompactionThread();

    return .{
        .allocator = allocator,
        .inner = inner,
    };
}

pub fn deinit(self: *Self) !void {
    try self.inner.syncDir();
    self.inner.deinit();
}

pub fn get(self: *Self, key: []const u8, val: *[]const u8) !bool {
    return self.inner.get(key, val);
}

pub fn put(self: *Self, key: []const u8, val: []const u8) !void {
    return self.inner.put(key, val);
}

pub fn del(self: *Self, key: []const u8) !void {
    return self.inner.delete(key);
}

pub fn writeBatch(self: *Self, batch: []const WriteBatchRecord) !void {
    return self.inner.writeBatch(batch);
}

pub fn sync(self: *Self) !void {
    return self.inner.sync();
}

pub fn scan(self: *Self, lower: Bound, upper: Bound) !LsmIterator {
    return self.inner.scan(lower, upper);
}

pub fn forceFlush(self: *Self) !void {
    var is_empty: bool = false;
    {
        self.inner.state_lock.lockShared();
        defer self.inner.state_lock.unlockShared();
        is_empty = self.inner.state.getMemTable().isEmpty();
    }

    if (!is_empty) {
        try self.inner.forceFreezeMemtable();
        try self.inner.flushNextMemtable();
    }
}

pub fn forceFullCompaction(self: *Self) !void {
    try self.inner.forceFullCompaction();
}
