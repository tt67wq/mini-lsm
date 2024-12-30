const std = @import("std");

pub const BlockBuilder = struct {
    allocator: std.mem.Allocator,
    offset_v: std.ArrayList(u16),
    data_v: std.ArrayList(u8),
    block_size: usize,
    first_key: []const u8,

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
            @memcpy(&self.first_key, key);
        }
        return true;
    }

    fn doAdd(self: *Self, key: []const u8, value: ?[]const u8) !void {
        // add the offset of the data into the offset array
        self.offset_v.append(@intCast(self.data_v.items.len));
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
        return Block.init(self.allocator, self.data_v, self.offset_v);
    }
};

pub const Block = struct {
    allocator: std.mem.Allocator,
    data_v: std.ArrayList(u8),
    offset_v: std.ArrayList(u16),

    pub fn init(allocator: std.mem.Allocator, data_v: std.ArrayList(u8), offset_v: std.ArrayList(u16)) Block {
        return Block{
            .allocator = allocator,
            .data_v = data_v,
            .offset_v = offset_v,
        };
    }

    pub fn deinit(self: *Block) void {
        self.data_v.deinit();
        self.offset_v.deinit();
    }

    pub fn encode(self: *Block, r: *[]const u8) !void {
        var buf = try self.data_v.clone();
        errdefer buf.deinit();

        const offset_len = self.offset_v.items.len;
        for (0..offset_len) |i| {
            try buf.writer().writeInt(u16, self.offset_v.itens[i], .big);
        }
        try buf.append(@intCast(offset_len));
        r.* = buf.toOwnedSlice();
    }

    // pub fn decode(data: []const u8) Block {
    //     var stream = std.io.fixedBufferStream(data);
    //     var bit_reader = std.io.bitReader(.big, stream.reader());
    //     var out_bits: usize = 0;
    //     bit_reader.readBits(u16, data.len - @sizeOf(u16), &out_bits);
    // }
};
