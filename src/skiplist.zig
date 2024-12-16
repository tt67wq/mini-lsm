const std = @import("std");

pub fn SkipList(comptime T: type) type {
    return struct {
        const CmpFn = fn (a: T, b: T) bool;
        const Node = struct {
            value: T,
            next: ?*Node,
            down: ?*Node,
        };
        const Self = @This();
        const SkipListError = error{};

        allocator: std.mem.Allocator,
        rng: std.Random,
        lt: *const CmpFn,
        max_levels: usize,
        levels: usize = 0,
        head: ?*Node = null,

        //  Descends to the node before `v` if it exists, or to the node before where `v` should be inserted.
        fn descend(self: *Self, v: T, levels: []*Node) *Node {
            if (self.head == null) {
                @panic("Cannot descend on empty list");
            }
            var level = self.levels - 1;
            var current = self.head.?;

            while (true) {
                const next = current.next;
                const next_is_greater_or_end = (next == null) or self.lt(v, next.?.value);
                if (!next_is_greater_or_end) {
                    current = next.?;
                    continue;
                }

                // If the `next` value is greater then `v` lies between `current` and `next`. If we are
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

        pub fn get(self: *Self, v: T, equal: *const CmpFn) ?*const T {
            const head = self.head orelse return null;
            if (equal(v, head.value)) return &head.value;
            if (self.lt(v, head.value)) return null;

            const levels = self.allocator.alloc(*Node, self.levels) catch unreachable;
            defer self.allocator.free(levels);
            const node = self.descend(v, levels);
            if (equal(v, node.value)) return &node.value;
            return null;
        }

        pub fn insert(self: *Self, v: T) !void {
            var node = try self.allocator.create(Node);
            errdefer self.allocator.destroy(node);

            node.* = .{
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

            // If `v` is less than the head of the list we need to create a new node and make it the new
            // head.
            if (self.lt(v, head.value)) {
                node.next = head;

                var head_current = head.down;
                var last = node;
                for (0..self.levels - 1) |_| {
                    const new_node = try self.allocator.create(Node);
                    errdefer self.allocator.destroy(new_node);

                    new_node.* = .{
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

            const prev = self.descend(v, levels);
            const next = prev.next;

            prev.next = node;
            node.next = next;

            const max_levels = @min(self.levels + 1, self.max_levels);
            const random = self.rng.int(u32);
            var down = node;
            for (1..max_levels) |l| {
                if (random & (@as(u32, 1) << @intCast(l)) == 0) break;

                const new_node = try self.allocator.create(Node);
                errdefer self.allocator.destroy(new_node);

                new_node.* = .{
                    .value = v,
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
                    .value = self.head.?.value,
                    .next = new_node,
                    .down = self.head,
                };
                self.head = new_head;
                return;
            }
        }
    };
}
