const std = @import("std");
const storage = @import("storage.zig");
const LsmIterator = @import("iterators.zig").LsmIterator;

pub const WriteBatchRecord = storage.WriteBatchRecord;
pub const StorageOptions = storage.StorageOptions;
pub const Bound = @import("MemTable.zig").Bound;

const Self = @This();

allocator: std.mem.Allocator,
inner: storage.StorageInnerPtr,

pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) !Self {
    var inner = try storage.StorageInner.init(allocator, path, options);
    errdefer inner.deinit();

    const ptr = try storage.StorageInnerPtr.create(allocator, inner);

    ptr.get().spawnFlushThread();
    ptr.get().spawnCompactionThread();

    return .{
        .allocator = allocator,
        .inner = ptr,
    };
}

pub fn deinit(self: *Self) !void {
    try self.inner.get().syncDir();
    self.inner.get().deinit();
}

pub fn get(self: *Self, key: []const u8, val: *[]const u8) !bool {
    return self.inner.get().get(key, val);
}

pub fn put(self: *Self, key: []const u8, val: []const u8) !void {
    return self.inner.get().put(key, val);
}

pub fn del(self: *Self, key: []const u8) !void {
    return self.inner.get().delete(key);
}

pub fn writeBatch(self: *Self, batch: []const WriteBatchRecord) !void {
    return self.inner.get().writeBatch(batch);
}

pub fn sync(self: *Self) !void {
    return self.inner.get().sync();
}

pub fn scan(self: *Self, lower: Bound, upper: Bound) !LsmIterator {
    return self.inner.get().scan(lower, upper);
}

pub fn forceFlush(self: *Self) !void {
    var is_empty: bool = false;
    {
        self.inner.get().state_lock.lockShared();
        defer self.inner.get().state_lock.unlockShared();
        is_empty = self.inner.get().state.getMemTable().isEmpty();
    }

    if (!is_empty) {
        try self.inner.get().forceFreezeMemtable();
        try self.inner.get().flushNextMemtable();
    }
}

pub fn forceFullCompaction(self: *Self) !void {
    try self.inner.get().forceFullCompaction();
}
