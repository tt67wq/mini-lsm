const std = @import("std");

pub const BlockBuilder = struct {
    allocator: std.mem.Allocator,
    offset_v: std.ArrayList(u16),
    data_v: std.ArrayList(u8),
    block_size: usize,
    first_key: []u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, block_size: usize) Self {
        return Self{
            .allocator = allocator,
            .offset_v = std.ArrayList(u16).init(allocator),
            .data_v = std.ArrayList(u8).init(allocator),
            .block_size = block_size,
            .first_key = "",
        };
    }

    pub fn deinit(self: *Self) void {
        self.offset_v.deinit();
        self.data_v.deinit();
        if (self.first_key.len > 0) self.allocator.free(self.first_key);
    }

    pub fn is_empty(self: Self) bool {
        return self.offset_v.items.len == 0;
    }

    fn estimated_size(self: Self) usize {
        return @sizeOf(u16) + self.offset_v.items.len * @sizeOf(u16) + self.data_v.items.len;
    }

    fn calculate_overlap(first_key: []const u8, key: []const u8) usize {
        var i: usize = 0;
        // prefix match
        while (true) : (i += 1) {
            if (i >= first_key.len or i >= key.len) {
                break;
            }
            if (first_key[i] != key[i]) {
                break;
            }
        }
        return i;
    }

    pub fn add(self: *Self, key: []const u8, value: ?[]const u8) bool {
        std.debug.assert(key.len > 0); // key must not be empty

        const vSize = if (value) |v| v.len else 0;
        if ((self.estimated_size() + key.len + vSize + 3 * @sizeOf(u16) > self.block_size) and !self.is_empty()) {
            return false;
        }
        self.doAdd(key, value) catch |err| {
            std.debug.panic("add {s} error: {any}", .{ key, err });
        };

        if (self.first_key.len == 0) {
            self.first_key = self.allocator.dupe(u8, key) catch |err| {
                std.debug.panic("dupe first key {s} error: {any}", .{ key, err });
            };
        }
        return true;
    }

    fn doAdd(self: *Self, key: []const u8, value: ?[]const u8) !void {
        // add the offset of the data into the offset array
        try self.offset_v.append(@intCast(self.data_v.items.len));
        const overlap = calculate_overlap(self.first_key, key);

        // encode key overlap
        try self.data_v.append(@intCast(overlap));
        // encode key length
        try self.data_v.append(@intCast(key.len - overlap));

        // encode key content
        try self.data_v.appendSlice(key[overlap..]);
        // encode value length
        if (value) |v| {
            try self.data_v.append(@intCast(v.len));
            // encode value content
            try self.data_v.appendSlice(v);
        } else {
            try self.data_v.append(0);
        }
    }

    pub fn build(self: Self) Block {
        if (self.is_empty()) {
            @panic("block is empty");
        }
        return Block.init(
            self.data_v.clone() catch |err| {
                std.debug.panic("clone data_v error: {any}", .{err});
            },
            self.offset_v.clone() catch |err| {
                std.debug.panic("clone offset_v error: {any}", .{err});
            },
        );
    }
};

pub const Block = struct {
    data_v: std.ArrayList(u8),
    offset_v: std.ArrayList(u16),

    pub fn init(data_v: std.ArrayList(u8), offset_v: std.ArrayList(u16)) Block {
        return Block{
            .data_v = data_v,
            .offset_v = offset_v,
        };
    }

    pub fn deinit(self: *Block) void {
        self.data_v.deinit();
        self.offset_v.deinit();
    }

    // ----------------------------------------------------------------------------------------------------
    // |             Data Section             |              Offset Section             |      Extra      |
    // ----------------------------------------------------------------------------------------------------
    // | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
    // ----------------------------------------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // |                           Entry #1                            | ... |
    // -----------------------------------------------------------------------
    // | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    // -----------------------------------------------------------------------

    // NOTICE: you have to free returned slice
    pub fn encode(self: *Block, allocator: std.mem.Allocator) ![]const u8 {
        var buf = try self.data_v.clone();
        defer buf.deinit();

        var bw = buf.writer();
        const offset_len = self.offset_v.items.len;
        for (0..offset_len) |i| {
            try bw.writeInt(u16, self.offset_v.items[i], .big);
        }
        try bw.writeInt(u16, @intCast(offset_len), .big);

        const r = try allocator.alloc(u8, buf.items.len);
        @memcpy(r, buf.items.ptr);

        return r;
    }

    pub fn decode(allocator: std.mem.Allocator, data: []const u8) !Block {
        const e_num_of_elements = data[data.len - @sizeOf(u16) ..];
        var stream = std.io.fixedBufferStream(e_num_of_elements);
        var reader = stream.reader();
        const num_of_elements = try reader.readInt(u16, .big);
        const offset_s_len = num_of_elements * @sizeOf(u16);
        const data_s_len = data.len - offset_s_len - @sizeOf(u16);

        var offset_v = std.ArrayList(u16).init(allocator);
        var data_v = std.ArrayList(u8).init(allocator);

        var offset_stream = std.io.fixedBufferStream(data[data_s_len .. data.len - @sizeOf(u16)]);
        var offset_reader = offset_stream.reader();

        while (true) {
            const offset = offset_reader.readInt(u16, .big) catch |err| {
                if (err == error.EndOfStream) {
                    break;
                }
                return err;
            };
            try offset_v.append(offset);
        }

        try data_v.appendSlice(data[0..data_s_len]);

        return Block.init(data_v, offset_v);
    }
};

test "block" {
    var bb = BlockBuilder.init(std.testing.allocator, 4096);
    defer bb.deinit();
    try std.testing.expect(bb.add("foo1", "bar1"));
    try std.testing.expect(bb.add("foo2", "bar2"));
    try std.testing.expect(bb.add("foo3", "bar3"));
    try std.testing.expect(bb.add("foo4", "bar4"));
    try std.testing.expect(bb.add("foo5", "bar5"));

    var b = bb.build();
    defer b.deinit();

    const eb = try b.encode(std.testing.allocator);
    defer std.testing.allocator.free(eb);

    var b2 = try Block.decode(std.testing.allocator, eb);
    defer b2.deinit();

    try std.testing.expectEqual(b.offset_v.items.len, b2.offset_v.items.len);
    try std.testing.expectEqual(b.data_v.items.len, b2.data_v.items.len);
}
