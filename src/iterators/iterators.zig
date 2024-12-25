const MemTable = @import("root").MemTable;
const MemTableIterator = MemTable.MemTableIterator;

pub const StorageIterator = union(enum) {
    mem_iter: MemTableIterator,

    pub fn hasNext(self: StorageIterator) bool {
        switch (self) {
            .mem_iter => |iter| {
                return iter.hasNext();
            },
        }
    }

    pub fn next(self: *StorageIterator) void {
        switch (self) {
            .mem_iter => |iter| {
                iter.next();
            },
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
