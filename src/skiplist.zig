const std = @import("std");
const bound = @import("bound.zig");

pub fn SkipList(comptime Tk: type, comptime Tv: type) type {
    return struct {
        const CmpFn = fn (a: Tk, b: Tk) bool;
        const Node = struct {
            key: Tk,
            value: ?Tv,
            next: ?*Node,
            down: ?*Node,
        };
        pub const Bound = bound.Bound(Tk);

        pub const Iterator = struct {
            current: ?*Node,
            upper_bound: Bound,
            lt: *const CmpFn,
            eq: *const CmpFn,

            pub fn init(current: ?*Node, upper_bound: Bound, lt: *const CmpFn, eq: *const CmpFn) Iterator {
                if (current) |c| {
                    switch (upper_bound.bound_t) {
                        .unbounded => {
                            return .{
                                .current = current,
                                .upper_bound = upper_bound,
                                .lt = lt,
                                .eq = eq,
                            };
                        },
                        .excluded => {
                            if (lt(c.key, upper_bound.data)) {
                                return .{
                                    .current = current,
                                    .upper_bound = upper_bound,
                                    .lt = lt,
                                    .eq = eq,
                                };
                            }
                        },
                        .included => {
                            if (lt(c.key, upper_bound.data) or eq(c.key, upper_bound.data)) {
                                return .{
                                    .current = current,
                                    .upper_bound = upper_bound,
                                    .lt = lt,
                                    .eq = eq,
                                };
                            }
                        },
                    }
                }
                return .{
                    .current = null,
                    .upper_bound = upper_bound,
                    .lt = lt,
                    .eq = eq,
                };
            }

            pub fn isEmpty(self: Iterator) bool {
                if (self.current) |_| {
                    return false;
                }
                return true;
            }

            pub fn key(self: Iterator) Tk {
                return self.current.?.key;
            }

            pub fn value(self: Iterator) ?Tv {
                return self.current.?.value;
            }

            pub fn next(self: *Iterator) void {
                self.current = self.current.?.next;
                if (self.current) |c| {
                    switch (self.upper_bound.bound_t) {
                        .unbounded => {},
                        .excluded => {
                            if (!self.lt(c.key, self.upper_bound.data)) {
                                self.current = null;
                            }
                        },
                        .included => {
                            if (!self.lt(c.key, self.upper_bound.data) and !self.eq(c.key, self.upper_bound.data)) {
                                self.current = null;
                            }
                        },
                    }
                }
            }
        };

        const Self = @This();

        allocator: std.mem.Allocator,
        rng: std.Random,
        lt: *const CmpFn,
        eq: *const CmpFn,
        max_levels: usize,
        levels: usize = 0,
        head: ?*Node = null,

        pub fn init(
            allocator: std.mem.Allocator,
            rng: std.Random,
            lt: *const CmpFn,
            eq: *const CmpFn,
        ) Self {
            return .{
                .allocator = allocator,
                .rng = rng,
                .lt = lt,
                .eq = eq,
                .max_levels = 16,
                .levels = 0,
                .head = null,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.head) |head| {
                var current = head;
                while (true) {
                    const down = current.down;
                    while (current.next) |next| {
                        const to_free = current;
                        current = next;
                        self.allocator.destroy(to_free);
                    }
                    self.allocator.destroy(current);

                    if (down) |d| {
                        current = d;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        pub fn display(self: *Self) void {
            std.debug.print("SkipList Levels: {d}\n", .{self.levels});
            if (self.head) |node| {
                var current = node;
                while (true) {
                    std.debug.print("{s} ", .{current.key});
                    const down = current.down;
                    while (current.next) |next| {
                        std.debug.print("{s} ", .{next.key});
                        current = next;
                    }
                    std.debug.print("\n", .{});
                    if (down) |d| {
                        current = d;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        fn descendMin(self: Self, levels: []*Node) *Node {
            if (self.head == null) {
                @panic("Cannot descend on empty list");
            }
            var level = self.levels - 1;
            var current = self.head.?;
            while (true) {
                const down = current.down;
                if (down) |d| {
                    levels[level] = current;
                    level -= 1;
                    current = d;
                    continue;
                } else {
                    return current;
                }
            }
        }

        // Find the biggest node less or equal to `key`.
        fn descend(self: Self, key: Tk, levels: []*Node) *Node {
            if (self.head == null) {
                @panic("Cannot descend on empty list");
            }
            var level = self.levels - 1;
            var current = self.head.?;

            while (true) {
                const next = current.next;
                const next_is_greater_or_end = (next == null) or self.lt(key, next.?.key);
                if (!next_is_greater_or_end) {
                    current = next.?;
                    continue;
                }

                // If the `next` key is greater then `v` lies between `current` and `next`. If we are
                // not at the base we descend, otherwise we are at the insertion point and can return.
                if (next_is_greater_or_end and level > 0) {
                    levels[level] = current;
                    level -= 1;
                    current = current.down.?;
                    continue;
                }

                return current;
            }
        }

        pub fn get(self: Self, key: Tk, value: *Tv) bool {
            const head = self.head orelse return false;
            if (self.eq(key, head.key)) {
                if (head.value) |v| value.* = v;
                return true;
            }
            if (self.lt(key, head.key)) return false;

            const levels = self.allocator.alloc(*Node, self.levels) catch unreachable;
            defer self.allocator.free(levels);
            const node = self.descend(key, levels);
            if (self.eq(key, node.key)) {
                if (node.value) |v| value.* = v;
                return true;
            }
            return false;
        }

        pub fn insert(self: *Self, k: Tk, v: ?Tv) !void {
            var node = try self.allocator.create(Node);
            errdefer self.allocator.destroy(node);

            node.* = .{
                .key = k,
                .value = v,
                .next = null,
                .down = null,
            };

            var head: *Node = undefined;
            if (self.head) |m| {
                head = m;
            } else {
                self.head = node;
                self.levels += 1;
                return;
            }

            // If `k` is less than the head of the list we need to create a new node and make it the new
            // head.
            if (self.lt(k, head.key)) {
                node.next = head;

                var head_current = head.down;
                var last = node;
                for (0..self.levels - 1) |_| {
                    const new_node = try self.allocator.create(Node);
                    errdefer self.allocator.destroy(new_node);

                    new_node.* = .{
                        .key = k,
                        .value = v,
                        .next = head_current,
                        .down = null,
                    };
                    last.down = new_node;
                    head_current = head_current.?.down;
                    last = new_node;
                }
                self.head = node;
                return;
            }

            const levels = try self.allocator.alloc(*Node, self.levels);
            defer self.allocator.free(levels);

            const prev = self.descend(k, levels);
            if (self.eq(prev.key, k)) {
                self.allocator.destroy(node);
                prev.value = v;
                return;
            }
            const next = prev.next;

            prev.next = node;
            node.next = next;

            // build index
            const max_levels = @min(self.levels + 1, self.max_levels);
            const random = self.rng.int(u32);
            var down = node;
            for (1..max_levels) |l| {
                if (random & (@as(u32, 1) << @intCast(l)) == 0) break;

                const new_node = try self.allocator.create(Node);
                errdefer self.allocator.destroy(new_node);

                new_node.* = .{
                    .key = k,
                    .value = null,
                    .next = null,
                    .down = down,
                };
                defer down = new_node;
                if (l < self.levels) {
                    const above = levels[l];
                    new_node.next = above.next;
                    above.next = new_node;
                    continue;
                }

                // We've created a new level so we need to make sure head is at this level too.
                self.levels += 1;

                const new_head = try self.allocator.create(Node);
                errdefer self.allocator.destroy(new_head);

                new_head.* = .{
                    .key = self.head.?.key,
                    .value = self.head.?.value,
                    .next = new_node,
                    .down = self.head,
                };
                self.head = new_head;
                return;
            }
        }

        // Returns an iterator over the range `(lower_bound, upper_bound)`.
        pub fn scan(self: Self, lower_bound: Bound, upper_bound: Bound) Iterator {
            if (!lower_bound.isUnbounded() and !upper_bound.isUnbounded() and self.lt(upper_bound.data, lower_bound.data)) @panic("invalid range");
            if (self.isEmpty()) {
                return Iterator.init(null, upper_bound, self.lt, self.eq);
            }

            const levels = self.allocator.alloc(*Node, self.levels) catch unreachable;
            defer self.allocator.free(levels);
            var node: *Node = undefined;
            if (lower_bound.isUnbounded()) {
                node = self.descendMin(levels);
            } else {
                node = self.descend(lower_bound.data, levels);
            }
            if (self.eq(node.key, lower_bound.data) and lower_bound.bound_t == .excluded) {
                return Iterator.init(node.next, upper_bound, self.lt, self.eq);
            }
            return Iterator.init(node, upper_bound, self.lt, self.eq);
        }

        pub fn isEmpty(self: Self) bool {
            if (self.head) |_| {
                return false;
            }
            return true;
        }
    };
}

fn testComp(a: u32, b: u32) bool {
    return a < b;
}

fn testEqual(a: u32, b: u32) bool {
    return a == b;
}

fn testCompBytes(a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn testEqualBytes(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

test "u8" {
    const allocator = std.testing.allocator;
    var rng = std.rand.DefaultPrng.init(0);
    const lt = SkipList(u32, u32);
    var list = lt.init(allocator, rng.random(), testComp, testEqual);
    defer list.deinit();
    for (0..64) |i| {
        if (i % 2 == 0) {
            try list.insert(@intCast(i), @intCast(i * 2));
        }
    }

    // list.display();

    var val: u32 = 0;
    if (!list.get(2, &val)) {
        unreachable;
    }

    var iter = list.scan(
        lt.Bound.init(16, .included),
        lt.Bound.init(32, .excluded),
    );
    while (!iter.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ iter.key(), iter.value().? });
        iter.next();
    }
}

test "bytes" {
    const allocator = std.testing.allocator;
    var rng = std.rand.DefaultPrng.init(0);
    const lt = SkipList([]const u8, []const u8);
    var list = lt.init(
        allocator,
        rng.random(),
        testCompBytes,
        testEqualBytes,
    );
    defer list.deinit();

    for (0..120) |i| {
        const key = std.fmt.allocPrint(allocator, "key{d:0>5}", .{i}) catch unreachable;
        const val = std.fmt.allocPrint(allocator, "val{d:0>5}", .{i}) catch unreachable;
        std.debug.print("insert {s} => {s}\n", .{ key, val });
        try list.insert(key, val);
    }

    // list.display();
    // std.debug.print("--------------------\n", .{});

    var iter = list.scan(
        lt.Bound.init("key", .included),
        lt.Bound.init("key00121", .excluded),
    );
    var to_free_key: []const u8 = undefined;
    var to_free_val: []const u8 = undefined;
    var first = true;
    while (!iter.isEmpty()) {
        std.debug.print("k={s} v={s}\n", .{ iter.key(), iter.value().? });
        if (!first) {
            allocator.free(to_free_key);
            allocator.free(to_free_val);
        }
        to_free_key = iter.key();
        to_free_val = iter.value().?;
        first = false;
        iter.next();
    }
    if (!first) {
        allocator.free(to_free_key);
        allocator.free(to_free_val);
    }

    std.debug.print("--------------------\n", .{});
}

test "iterator" {
    const allocator = std.testing.allocator;
    var rng = std.rand.DefaultPrng.init(0);
    const List = SkipList(u32, u32);
    var list = List.init(allocator, rng.random(), testComp, testEqual);
    defer list.deinit();
    for (10..20) |i| {
        try list.insert(@intCast(i), @intCast(i));
    }

    var it = list.scan(List.Bound.init(10, .included), List.Bound.init(19, .included));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(10, .excluded), List.Bound.init(19, .included));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(10, .excluded), List.Bound.init(19, .excluded));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(5, .excluded), List.Bound.init(10, .included));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(5, .excluded), List.Bound.init(10, .excluded));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(19, .excluded), List.Bound.init(20, .included));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(19, .included), List.Bound.init(20, .included));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(19, .unbounded), List.Bound.init(18, .included));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }

    std.debug.print("-----------------------------\n", .{});

    it = list.scan(List.Bound.init(19, .unbounded), List.Bound.init(18, .unbounded));
    while (!it.isEmpty()) {
        std.debug.print("k={d} v={d}\n", .{ it.key(), it.value().? });
        it.next();
    }
}
