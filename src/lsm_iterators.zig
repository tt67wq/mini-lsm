const std = @import("std");
const MergeIterators = @import("MergeIterators.zig");

const LsmIteratorInner = MergeIterators;

pub const Bound = struct {
    bound: []const u8,
    bound_type: enum {
        unbounded,
        included,
        excluded,
    },
};

pub const LsmIterator = struct {
    allocator: std.mem.Allocator,
    inner: LsmIteratorInner,
    end_bound: Bound,
    is_empty: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        inner: LsmIteratorInner,
        end_bound: Bound,
    ) LsmIterator {
        return LsmIterator{
            .allocator = allocator,
            .inner = inner,
            .end_bound = end_bound,
            .is_empty = inner.isEmpty(),
        };
    }

    pub fn deinit(self: *LsmIterator) void {
        self.inner.deinit();
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

    pub fn next(self: *LsmIterator) void {
        self.inner.next();
        if (self.inner.isEmpty()) {
            self.is_empty = true;
            return;
        }
        switch (self.end_bound.bound_type) {
            .unbounded => return,
            .included => {
                self.is_empty = std.mem.lessThan(u8, self.key(), self.end_bound.bound) or
                    std.mem.eql(u8, self.key(), self.end_bound.bound);
                return;
            },
            .excluded => {
                self.is_empty = std.mem.lessThan(u8, self.key(), self.end_bound.bound);
                return;
            },
        }
    }
};
