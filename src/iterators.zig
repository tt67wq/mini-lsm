const std = @import("std");
const MemTable = @import("MemTable.zig");
const MemTableIterator = MemTable.MemTableIterator;
const lsm_iterator = @import("lsm_iterators.zig");
const LsmIterator = lsm_iterator.LsmIterator;

pub const StorageIterator = union(enum) {
    mem_iter: MemTableIterator,
    lsm_iter: LsmIterator,

    pub fn isEmpty(self: StorageIterator) bool {
        switch (self) {
            .mem_iter => |iter| {
                return iter.isEmpty();
            },
            .lsm_iter => |iter| {
                return iter.isEmpty();
            },
        }
    }

    pub fn next(self: *StorageIterator) void {
        switch (self.*) {
            .mem_iter => self.mem_iter.next(),
            .lsm_iter => self.lsm_iter.next(),
        }
    }

    pub fn key(self: StorageIterator) []const u8 {
        switch (self) {
            .mem_iter => |iter| {
                return iter.key();
            },
            .lsm_iter => |iter| {
                return iter.key();
            },
        }
    }

    pub fn value(self: StorageIterator) ?[]const u8 {
        switch (self) {
            .mem_iter => |iter| {
                return iter.value();
            },
            .lsm_iter => |iter| {
                return iter.value();
            },
        }
    }
};
