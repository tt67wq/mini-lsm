const std = @import("std");
const SkipListError = error{};

pub fn SkipList(comptime Tk: type, comptime Tv: type) type {
    return struct {
        const CmpFn = fn (a: Tk, b: Tk) bool;
        const Node = struct {
            key: Tk,
            value: ?Tv,
            next: ?*Node,
            down: ?*Node,
        };

        pub const Iterator = struct {
            current: Node,
            upper_bound: Tk,
            lt: *const CmpFn,

            pub fn init(current: ?*Node, upper_bound: Tk, lt: *const CmpFn) Iterator {
                const init_node = Node{
                    .key = undefined,
                    .value = undefined,
                    .next = current,
                    .down = undefined,
                };
                return .{
                    .current = init_node,
                    .upper_bound = upper_bound,
                    .lt = lt,
                };
            }

            pub fn hasNext(self: Iterator) bool {
                if (self.current.next) |n| {
                    return self.lt(n.key, self.upper_bound);
                }
                return false;
            }

            pub fn next(self: *Iterator) ?struct {
                key: Tk,
                value: ?Tv,
            } {
                if (self.current.next) |n| {
                    self.current = n.*;
                    return .{
                        .key = n.key,
                        .value = n.value,
                    };
                }
                return null;
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

        //  Descends to the node before `key` if it exists, or to the node before where `v` should be inserted.
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

        // Returns an iterator over the range `[lower_bound, upper_bound)`.
        pub fn iter(self: Self, lower_bound: Tk, upper_bound: Tk) Iterator {
            const levels = self.allocator.alloc(*Node, self.levels) catch unreachable;
            defer self.allocator.free(levels);
            const node = self.descend(lower_bound, levels);
            if (self.lt(node.key, lower_bound)) {
                return Iterator.init(null, upper_bound, self.lt);
            }
            return Iterator.init(node, upper_bound, self.lt);
        }

        pub fn is_empty(self: Self) bool {
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

test "skip list u8" {
    const allocator = std.testing.allocator;
    var rng = std.rand.DefaultPrng.init(0);
    var list = SkipList(u32, u32).init(allocator, rng.random(), testComp, testEqual);
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

    var iter = list.iter(16, 32);
    while (iter.hasNext()) {
        const m = iter.next().?;
        std.debug.print("k={d} v={d}\n", .{ m.key, m.value.? });
    }

    iter = list.iter(65, 100);
    try std.testing.expect(!iter.hasNext());
}

test "skip list bytes" {
    const allocator = std.testing.allocator;
    var rng = std.rand.DefaultPrng.init(0);
    var list = SkipList([]const u8, []const u8).init(
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

    var iter = list.iter("key", "key00121");
    var to_free_key: []const u8 = undefined;
    var to_free_val: []const u8 = undefined;
    var first = true;
    while (iter.hasNext()) {
        const m = iter.next().?;
        std.debug.print("k={s} v={s}\n", .{ m.key, m.value.? });
        if (!first) {
            allocator.free(to_free_key);
            allocator.free(to_free_val);
        }
        to_free_key = m.key;
        to_free_val = m.value.?;
        first = false;
    }
    if (!first) {
        allocator.free(to_free_key);
        allocator.free(to_free_val);
    }

    std.debug.print("--------------------\n", .{});
}
