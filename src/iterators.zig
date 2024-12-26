const std = @import("std");
const MemTable = @import("MemTable.zig");
const MemTableIterator = MemTable.MemTableIterator;

pub const StorageIterator = union(enum) {
    mem_iter: MemTableIterator,

    pub fn isEmpty(self: StorageIterator) bool {
        switch (self) {
            .mem_iter => |iter| {
                return iter.isEmpty();
            },
        }
    }

    pub fn next(self: *StorageIterator) void {
        switch (self.*) {
            .mem_iter => self.mem_iter.next(),
        }
    }

    pub fn key(self: StorageIterator) []const u8 {
        switch (self) {
            .mem_iter => |iter| {
                return iter.key();
            },
        }
    }

    pub fn value(self: StorageIterator) ?[]const u8 {
        switch (self) {
            .mem_iter => |iter| {
                return iter.value();
            },
        }
    }
};
