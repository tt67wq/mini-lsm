const std = @import("std");
const MemTable = @import("MemTable.zig");
const ss_table = @import("ss_table.zig");
const MergeIterators = @import("MergeIterators.zig");
const Bound = @import("MemTable.zig").Bound;
const MemTableIterator = MemTable.MemTableIterator;
const SsTableIterator = ss_table.SsTableIterator;

pub const StorageIterator = union(enum) {
    mem_iter: MemTableIterator,
    ss_table_iter: SsTableIterator,

    pub fn deinit(self: *StorageIterator) void {
        switch (self.*) {
            .ss_table_iter => self.ss_table_iter.deinit(),
            inline else => {},
        }
    }

    pub fn isEmpty(self: StorageIterator) bool {
        switch (self) {
            inline else => |impl| return impl.isEmpty(),
        }
    }

    pub fn next(self: *StorageIterator) void {
        switch (self.*) {
            .mem_iter => self.mem_iter.next(),
            .ss_table_iter => self.ss_table_iter.next(),
        }
    }

    pub fn key(self: StorageIterator) []const u8 {
        switch (self) {
            inline else => |impl| return impl.key(),
        }
    }

    pub fn value(self: StorageIterator) []const u8 {
        switch (self) {
            inline else => |impl| return impl.value(),
        }
    }

    pub fn numActiveIterators(_: StorageIterator) usize {
        return 1;
    }
};

pub const TwoMergeIterator = struct {
    a: StorageIterator,
    b: StorageIterator,
    choose_a: bool,

    fn chooseA(a: StorageIterator, b: StorageIterator) bool {
        if (a.isEmpty()) {
            return false;
        }
        if (b.isEmpty()) {
            return true;
        }
        return std.mem.lessThan(u8, a.key(), b.key());
    }

    fn skipB(self: *TwoMergeIterator) void {
        if (!self.a.isEmpty() and !self.b.isEmpty() and std.mem.eql(u8, self.a.key(), self.b.key())) self.b.next();
    }

    fn init(a: StorageIterator, b: StorageIterator) TwoMergeIterator {
        var iter = TwoMergeIterator{
            .a = a,
            .b = b,
            .choose_a = false,
        };
        iter.skipB();
        iter.choose_a = chooseA(a, b);
        return iter;
    }

    fn deinit(_: *TwoMergeIterator) void {}

    pub fn key(self: TwoMergeIterator) []const u8 {
        if (self.choose_a) {
            std.debug.assert(!self.a.isEmpty());
            return self.a.key();
        }
        std.debug.assert(!self.b.isEmpty());
        return self.b.key();
    }

    pub fn value(self: TwoMergeIterator) []const u8 {
        if (self.choose_a) {
            std.debug.assert(!self.a.isEmpty());
            return self.a.value();
        }
        std.debug.assert(!self.b.isEmpty());
        return self.b.value();
    }

    pub fn isEmpty(self: TwoMergeIterator) bool {
        if (self.choose_a) {
            return self.a.isEmpty();
        }
        return self.b.isEmpty();
    }

    pub fn next(self: *TwoMergeIterator) void {
        if (self.choose_a) {
            self.a.next();
        } else {
            self.b.next();
        }
        self.skipB();
        self.choose_a = chooseA(self.a, self.b);
    }

    pub fn numActiveIterators(self: TwoMergeIterator) usize {
        return self.a.numActiveIterators() + self.b.numActiveIterators();
    }
};

const LsmIteratorInner = TwoMergeIterator;

pub const LsmIterator = struct {
    inner: LsmIteratorInner,
    end_bound: Bound,
    is_empty: bool,

    pub fn init(
        inner: LsmIteratorInner,
        end_bound: Bound,
    ) LsmIterator {
        return LsmIterator{
            .inner = inner,
            .end_bound = end_bound,
            .is_empty = inner.isEmpty(),
        };
    }

    pub fn deinit(self: *LsmIterator) void {
        self.inner.deinit();
    }

    fn nextInner(self: *LsmIterator) void {
        self.inner.next();
        if (self.inner.isEmpty()) {
            self.is_empty = true;
            return;
        }
        switch (self.end_bound.bound_t) {
            .unbounded => {},
            .included => {
                self.is_empty = std.mem.lessThan(u8, self.key(), self.end_bound.data) or
                    std.mem.eql(u8, self.key(), self.end_bound.data);
            },
            .excluded => {
                self.is_empty = std.mem.lessThan(u8, self.key(), self.end_bound.data);
            },
        }
        return;
    }

    fn moveToNoneDelete(self: *LsmIterator) void {
        while (!self.isEmpty() and self.inner.value().len == 0) {
            self.nextInner();
        }
    }

    pub fn next(self: *LsmIterator) void {
        self.nextInner();
        self.moveToNoneDelete();
    }

    pub fn isEmpty(self: LsmIterator) bool {
        return self.is_empty;
    }

    pub fn key(self: LsmIterator) []const u8 {
        return self.inner.key();
    }

    pub fn value(self: LsmIterator) ?[]const u8 {
        return self.inner.value();
    }

    pub fn numActiveIterators(self: LsmIterator) usize {
        return self.inner.numActiveIterators();
    }
};
