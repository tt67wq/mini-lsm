const std = @import("std");
const StorageIterator = @import("iterators.zig").StorageIterator;

const Self = @This();

const HeapWrapper = struct {
    id: usize,
    ee: StorageIterator,

    pub fn init(id: usize, ee: StorageIterator) HeapWrapper {
        return HeapWrapper{ .id = id, .ee = ee };
    }

    pub fn hasNext(self: HeapWrapper) bool {
        return self.ee.hasNext();
    }

    pub fn next(self: *HeapWrapper) void {
        self.ee.next();
    }

    pub fn key(self: HeapWrapper) []const u8 {
        return self.ee.key();
    }

    pub fn value(self: HeapWrapper) ?[]const u8 {
        return self.ee.value();
    }

    pub fn lessThan(self: HeapWrapper, other: HeapWrapper) bool {
        return Comparer.cmp(null, self, other) == .lt;
    }
};

const Comparer = struct {
    fn cmpIter(a: StorageIterator, b: StorageIterator) std.math.Order {
        if (std.mem.eql(u8, a.key(), b.key())) return .eq;
        if (std.mem.lessThan(u8, a.key(), b.key())) return .lt;
        return .gt;
    }

    fn cmpId(a: usize, b: usize) std.math.Order {
        if (a < b) return .lt;
        if (a > b) return .gt;
        return .eq;
    }

    pub fn cmp(_: void, a: HeapWrapper, b: HeapWrapper) std.math.Order {
        const c1 = cmpIter(a.ee, b.ee);
        if (c1 == .eq) {
            return .cmpId(a.id, b.id);
        }
        return c1;
    }
};

const IteratorHeap = std.PriorityQueue(HeapWrapper, struct {}, Comparer.cmp);

allocator: std.mem.Allocator,
q: IteratorHeap,
current: ?HeapWrapper,

pub fn init(allocator: std.mem.Allocator, iters: std.ArrayList(StorageIterator)) Self {
    var q = IteratorHeap.init(allocator, null);
    if (iters.items.len == 0) {
        return Self{
            .allocator = allocator,
            .q = q,
            .current = null,
        };
    }

    for (iters.items, 0..) |iter, i| {
        if (iter.hasNext()) {
            q.add(HeapWrapper.init(i, iter)) catch unreachable;
        }
    }
    return Self{
        .allocator = allocator,
        .q = q,
        .current = q.removeOrNull(),
    };
}

pub fn deinit(self: *Self) void {
    self.q.deinit();
}

pub fn key(self: Self) []const u8 {
    return self.current.?.key();
}

pub fn value(self: Self) ?[]const u8 {
    return self.current.?.value();
}

pub fn hasNext(self: Self) bool {
    if (self.current) |cc| {
        return cc.hasNext();
    }
    return false;
}

pub fn next(self: *Self) void {
    var cc = self.current.?;
    while (true) {
        if (self.q.peek()) |ii| {
            if (!ii.hasNext()) unreachable;
            if (std.mem.eql(u8, cc.key(), ii.key())) {
                ii.next();
                if (!ii.hasNext()) {
                    _ = self.q.remove();
                }
            } else {
                break;
            }
        }
        break;
    }

    cc.next();

    if (!cc.hasNext()) {
        self.current = self.q.removeOrNull();
        return;
    }

    if (self.q.peek()) |ii| {
        if (cc.lessThan(ii)) {
            self.current = ii;
            self.q.update(ii, cc) catch unreachable;
        }
    }
}
