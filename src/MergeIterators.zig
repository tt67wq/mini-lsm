const std = @import("std");
const iterators = @import("iterators.zig");
const StorageIterator = iterators.StorageIterator;
const StorageIteratorPtr = iterators.StorageIteratorPtr;

const Self = @This();

const HeapWrapper = struct {
    id: usize,
    ee: StorageIteratorPtr,

    pub fn init(id: usize, ee: StorageIteratorPtr) HeapWrapper {
        return HeapWrapper{ .id = id, .ee = ee };
    }

    pub fn deinit(self: *HeapWrapper) void {
        self.ee.release();
    }

    pub fn isEmpty(self: HeapWrapper) bool {
        return self.ee.get().isEmpty();
    }

    pub fn next(self: *HeapWrapper) !void {
        try self.ee.get().next();
    }

    pub fn key(self: HeapWrapper) []const u8 {
        return self.ee.get().*.key();
    }

    pub fn value(self: HeapWrapper) []const u8 {
        return self.ee.get().*.value();
    }

    pub fn lessThan(self: *HeapWrapper, other: *HeapWrapper) bool {
        return Comparer.cmp(.{}, self, other) == .lt;
    }
};

const Comparer = struct {
    pub const Context = struct {};
    fn cmpIter(a: StorageIterator, b: StorageIterator) std.math.Order {
        if (std.mem.eql(u8, a.key(), b.key())) return .eq;
        if (std.mem.lessThan(u8, a.key(), b.key())) return .lt;
        return .gt;
    }

    fn cmpId(a: usize, b: usize) std.math.Order {
        return std.math.order(b, a);
    }

    pub fn cmp(_: Context, a: *HeapWrapper, b: *HeapWrapper) std.math.Order {
        const c1 = cmpIter(a.ee.get().*, b.ee.get().*);
        if (c1 == .eq) {
            return cmpId(a.id, b.id);
        }
        return c1;
    }
};

const IteratorHeap = std.PriorityQueue(*HeapWrapper, Comparer.Context, Comparer.cmp);

allocator: std.mem.Allocator,
q: IteratorHeap,
current: ?*HeapWrapper,

pub fn init(allocator: std.mem.Allocator, iters: std.ArrayList(StorageIteratorPtr)) !Self {
    var q = IteratorHeap.init(allocator, .{});
    if (iters.items.len == 0) {
        return Self{
            .allocator = allocator,
            .q = q,
            .current = null,
        };
    }

    // PS: the last iter has the highest priority
    for (iters.items, 0..) |sp, i| {
        if (!sp.load().isEmpty()) {
            const hw = try allocator.create(HeapWrapper);
            errdefer allocator.destroy(hw);
            hw.* = HeapWrapper.init(i, sp.clone());
            try q.add(hw);
        }
    }

    const cc = q.removeOrNull();
    return Self{
        .allocator = allocator,
        .q = q,
        .current = cc,
    };
}

pub fn deinit(self: *Self) void {
    if (self.current) |cc| {
        cc.deinit();
        self.allocator.destroy(cc);
    }
    var it = self.q.iterator();
    while (true) {
        if (it.next()) |h| {
            h.deinit();
            self.allocator.destroy(h);
        } else {
            break;
        }
    }
    self.q.deinit();
}

pub fn key(self: Self) []const u8 {
    return self.current.?.key();
}

pub fn value(self: Self) []const u8 {
    return self.current.?.value();
}

pub fn isEmpty(self: Self) bool {
    if (self.current) |cc| {
        return cc.isEmpty();
    }
    return true;
}

pub fn next(self: *Self) !void {
    const cc = self.current.?;
    while (true) {
        if (self.q.peek()) |ii| {
            std.debug.assert(!ii.isEmpty());
            if (std.mem.eql(u8, cc.key(), ii.key())) {
                try ii.next();
                if (ii.isEmpty()) {
                    _ = self.q.remove();
                    ii.deinit();
                    self.allocator.destroy(ii);
                }
            } else {
                break;
            }
        }
        break;
    }

    try cc.next();

    if (cc.isEmpty()) {
        defer {
            cc.deinit();
            self.allocator.destroy(cc);
        }
        if (self.q.removeOrNull()) |h| {
            self.current = h;
        } else {
            self.current = null;
        }
        return;
    }

    try self.q.add(cc);
    self.current = self.q.removeOrNull();
}

pub fn numActiveIterators(self: Self) usize {
    var s = 0;
    var it = self.q.iterator();
    while (true) {
        if (it.next()) |h| {
            s += h.ee.numActiveIterators();
            continue;
        }
        break;
    }
    if (self.current) |h| {
        s += h.ee.numActiveIterators();
    }
    return s;
}

test "merge_iterator" {
    const MemTable = @import("MemTable.zig");
    defer std.fs.cwd().deleteTree("./tmp/mm") catch unreachable;
    const allocator = std.testing.allocator;
    var m1 = MemTable.init(0, allocator, "./tmp/mm/1");
    defer m1.deinit();
    try m1.put("e", "4");

    var m2 = MemTable.init(0, allocator, "./tmp/mm/2");
    defer m2.deinit();
    try m2.put("a", "1");
    try m2.put("b", "2");
    try m2.put("c", "3");

    var m3 = MemTable.init(0, allocator, "./tmp/mm/3");
    defer m3.deinit();
    try m3.put("b", "");
    try m3.put("c", "4");
    try m3.put("d", "5");

    var iters = std.ArrayList(StorageIteratorPtr).init(allocator);
    defer {
        for (iters.items) |sp| {
            var spp = sp;
            spp.release();
        }
        iters.deinit();
    }

    const bound_a = MemTable.Bound.init("a", .included);
    const bound_z = MemTable.Bound.init("z", .included);

    var s1 = try StorageIteratorPtr.create(allocator, StorageIterator{ .mem_iter = m1.scan(bound_a, bound_z) });
    defer s1.release();

    var s2 = try StorageIteratorPtr.create(allocator, StorageIterator{ .mem_iter = m2.scan(bound_a, bound_z) });
    defer s2.release();

    var s3 = try StorageIteratorPtr.create(allocator, StorageIterator{ .mem_iter = m3.scan(bound_a, bound_z) });
    defer s3.release();

    try iters.append(s1.clone());
    try iters.append(s2.clone());
    try iters.append(s3.clone());

    // 2 iter1: b->del, c->4, d->5
    // 1 iter2: a->1, b->2, c->3
    // 0 iter3: e->4

    var mit = try Self.init(allocator, iters);
    defer mit.deinit();

    while (!mit.isEmpty()) {
        std.debug.print("key: {s}, value: {s}\n", .{ mit.key(), mit.value() });
        try mit.next();
    }
}
