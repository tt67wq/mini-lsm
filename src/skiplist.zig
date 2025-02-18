const std = @import("std");
const bound = @import("bound.zig");

const Node = struct {
    key: []const u8,
    value: []const u8,
    next: ?*Node,
    down: ?*Node,
};
pub const Bound = bound.Bound([]const u8);

pub const Iterator = struct {
    current: ?*Node,
    upper_bound: Bound,

    pub fn init(current: ?*Node, upper_bound: Bound) Iterator {
        if (current) |c| {
            switch (upper_bound.bound_t) {
                .unbounded => {
                    return .{
                        .current = current,
                        .upper_bound = upper_bound,
                    };
                },
                .excluded => {
                    if (std.mem.lessThan(u8, c.key, upper_bound.data)) {
                        return .{
                            .current = current,
                            .upper_bound = upper_bound,
                        };
                    }
                },
                .included => {
                    if (std.mem.lessThan(u8, c.key, upper_bound.data) or std.mem.eql(u8, c.key, upper_bound.data)) {
                        return .{
                            .current = current,
                            .upper_bound = upper_bound,
                        };
                    }
                },
            }
        }
        return .{
            .current = null,
            .upper_bound = upper_bound,
        };
    }

    pub fn isEmpty(self: Iterator) bool {
        if (self.current) |_| {
            return false;
        }
        return true;
    }

    pub fn key(self: Iterator) []const u8 {
        return self.current.?.key;
    }

    pub fn value(self: Iterator) []const u8 {
        return self.current.?.value;
    }

    pub fn next(self: *Iterator) void {
        self.current = self.current.?.next;
        if (self.current) |c| {
            switch (self.upper_bound.bound_t) {
                .unbounded => {},
                .excluded => {
                    if (!std.mem.lessThan(u8, c.key, self.upper_bound.data)) {
                        self.current = null;
                    }
                },
                .included => {
                    if (!std.mem.lessThan(u8, c.key, self.upper_bound.data) and !std.mem.eql(u8, c.key, self.upper_bound.data)) {
                        self.current = null;
                    }
                },
            }
        }
    }
};

const Self = @This();

allocator: std.mem.Allocator,
arena: std.heap.ArenaAllocator,
rng: std.Random,
max_levels: usize,
levels: usize = 0,
head: ?*Node = null,
first_key: ?[]const u8 = null,
last_key: ?[]const u8 = null,

pub fn init(allocator: std.mem.Allocator, rng: std.Random) Self {
    return .{
        .allocator = allocator,
        .arena = std.heap.ArenaAllocator.init(allocator),
        .rng = rng,
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
    self.arena.deinit();
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
fn descend(self: Self, key: []const u8, levels: []*Node) *Node {
    if (self.head == null) {
        @panic("Cannot descend on empty list");
    }
    var level = self.levels - 1;
    var current = self.head.?;

    while (true) {
        const next = current.next;
        const next_is_greater_or_end = (next == null) or std.mem.lessThan(u8, key, next.?.key);
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

pub fn get(self: Self, key: []const u8, value: *[]const u8) !bool {
    const head = self.head orelse return false;
    if (self.eq(key, head.key)) {
        if (head.value) |v| value.* = v;
        return true;
    }
    if (std.mem.lessThan(u8, key, head.key)) return false;

    const levels = try self.allocator.alloc(*Node, self.levels);
    defer self.allocator.free(levels);
    const node = self.descend(key, levels);
    if (self.eq(key, node.key)) {
        if (node.value) |v| value.* = v;
        return true;
    }
    return false;
}

fn setFirstKey(self: *Self, key: []const u8) void {
    if (self.first_key) |_| {
        if (std.mem.lessThan(u8, key, self.first_key.?)) {
            // self.first_key = try self.arena.allocator().dupe(u8, key);
            self.first_key = key;
            return;
        }
    } else {
        self.first_key = key;
    }
}

fn setLastKey(self: *Self, key: []const u8) void {
    if (self.last_key) |_| {
        if (std.mem.lessThan(u8, self.last_key.?, key)) {
            self.last_key = key;
            return;
        }
    } else {
        self.last_key = key;
    }
}

pub fn firstKey(self: Self) ?[]const u8 {
    return self.first_key;
}

pub fn lastKey(self: Self) ?[]const u8 {
    return self.last_key;
}

pub fn insert(self: *Self, k: []const u8, v: []const u8) !void {
    var node = try self.allocator.create(Node);
    errdefer self.allocator.destroy(node);

    const kk = try self.arena.allocator().dupe(u8, k);
    const vv = try self.arena.allocator().dupe(u8, v);

    defer {
        self.setFirstKey(kk);
        self.setLastKey(kk);
    }

    node.* = .{
        .key = kk,
        .value = vv,
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
    if (std.mem.lessThan(u8, k, head.key)) {
        node.next = head;

        var head_current = head.down;
        var last = node;
        for (0..self.levels - 1) |_| {
            const new_node = try self.allocator.create(Node);
            errdefer self.allocator.destroy(new_node);

            new_node.* = .{
                .key = kk,
                .value = vv,
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
    if (std.mem.eql(u8, prev.key, k)) {
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
            .key = kk,
            .value = vv,
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

fn rangeOverlap(self: Self, lower_bound: Bound, upper_bound: Bound) bool {
    const fk = self.firstKey().?;
    const lk = self.lastKey().?;
    switch (upper_bound.bound_t) {
        .excluded => {
            // upper <= first
            // !upper > first
            if (!std.mem.lessThan(u8, fk, upper_bound.data)) return false;
        },
        .included => {
            // upper < first
            if (std.mem.lessThan(u8, upper_bound.data, fk)) return false;
        },
        .unbounded => {},
    }
    switch (lower_bound.bound_t) {
        .excluded => {
            // lower >= last
            // !lower < last
            if (!std.mem.lessThan(u8, lower_bound.data, lk)) return false;
        },
        .included => {
            // lower > last
            if (std.mem.lessThan(u8, lk, lower_bound.data)) return false;
        },
        .unbounded => {},
    }
    return true;
}

// Returns an iterator over the range `(lower_bound, upper_bound)`.
pub fn scan(self: Self, lower_bound: Bound, upper_bound: Bound) Iterator {
    if (self.isEmpty()) {
        return Iterator.init(null, upper_bound);
    }

    if (!self.rangeOverlap(lower_bound, upper_bound)) {
        return Iterator.init(null, upper_bound);
    }

    const levels = self.allocator.alloc(*Node, self.levels) catch unreachable;
    defer self.allocator.free(levels);
    var node: *Node = undefined;
    if (lower_bound.isUnbounded()) {
        node = self.descendMin(levels);
    } else {
        node = self.descend(lower_bound.data, levels);
    }

    return Iterator.init(node, upper_bound);
}

pub fn isEmpty(self: Self) bool {
    if (self.head) |_| {
        return false;
    }
    return true;
}

test "skiplist" {
    const allocator = std.testing.allocator;
    var rng = std.rand.DefaultPrng.init(0);
    var list = Self.init(allocator, rng.random());
    defer list.deinit();

    for (0..16) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});

        std.debug.print("insert {s} => {s}\n", .{ kk, vv });
        try list.insert(kk, vv);
    }

    const fk = list.firstKey().?;
    const lk = list.lastKey().?;
    std.debug.print("first key: {s}, last key: {s}\n", .{ fk, lk });

    // list.display();
    // std.debug.print("--------------------\n", .{});

    var iter = list.scan(
        Bound.init("key00008", .included),
        Bound.init("key00013", .excluded),
    );
    while (!iter.isEmpty()) {
        std.debug.print("k={s} v={s}\n", .{ iter.key(), iter.value() });
        iter.next();
    }
}
