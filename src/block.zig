const std = @import("std");
const smart_pointer = @import("smart_pointer.zig");

pub const BlockBuilderPtr = smart_pointer.SmartPointer(BlockBuilder);

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

    pub fn isEmpty(self: Self) bool {
        return self.offset_v.items.len == 0;
    }

    fn estimatedSize(self: Self) usize {
        return @sizeOf(u16) + self.offset_v.items.len * @sizeOf(u16) + self.data_v.items.len;
    }

    pub fn reset(self: *Self) void {
        self.offset_v.clearRetainingCapacity();
        self.data_v.clearRetainingCapacity();
        if (self.first_key.len > 0) self.allocator.free(self.first_key);
        self.first_key = "";
    }

    fn calculateOverlap(first_key: []const u8, key: []const u8) usize {
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

    pub fn add(self: *Self, key: []const u8, value: ?[]const u8) !bool {
        std.debug.assert(key.len > 0); // key must not be empty

        const vSize = if (value) |v| v.len else 0;
        if ((self.estimatedSize() + key.len + vSize + 3 * @sizeOf(u16) > self.block_size) and !self.isEmpty()) {
            return false;
        }
        try self.doAdd(key, value);

        if (self.first_key.len == 0) {
            self.first_key = try self.allocator.dupe(u8, key);
        }
        return true;
    }

    fn doAdd(self: *Self, key: []const u8, value: ?[]const u8) !void {
        // add the offset of the data into the offset array
        try self.offset_v.append(@intCast(self.data_v.items.len));
        const overlap = calculateOverlap(self.first_key, key);

        var dw = self.data_v.writer();
        // encode key overlap
        try dw.writeInt(u16, @intCast(overlap), .big);
        // encode key length
        try dw.writeInt(u16, @intCast(key.len - overlap), .big);

        // encode key content
        _ = try dw.write(key[overlap..]);
        // encode value length
        if (value) |v| {
            try dw.writeInt(u16, @intCast(v.len), .big);
            // encode value content
            _ = try dw.write(v);
        } else {
            try dw.writeInt(u16, 0, .big);
        }
    }

    pub fn build(self: *Self) !Block {
        if (self.isEmpty()) {
            @panic("block is empty");
        }
        return Block.init(
            try self.data_v.clone(),
            try self.offset_v.clone(),
        );
    }
};

pub const BlockPtr = smart_pointer.SmartPointer(Block);

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

    pub fn getFirstKey(self: Block, allocator: std.mem.Allocator) ![]u8 {
        var stream = std.io.fixedBufferStream(self.data_v.items);
        var reader = stream.reader();
        _ = try reader.readInt(u16, .big);
        const key_len = try reader.readInt(u16, .big);

        const key = try allocator.alloc(u8, @intCast(key_len));
        errdefer allocator.free(key);
        _ = try reader.read(key);
        return key;
    }

    fn asInt(comptime T: type, bytes: []const u8) T {
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .big);
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
    pub fn encode(self: Block, allocator: std.mem.Allocator) ![]const u8 {
        var buf = try self.data_v.clone();
        defer buf.deinit();

        var bw = buf.writer();
        const offset_len = self.offset_v.items.len;
        for (0..offset_len) |i| {
            try bw.writeInt(u16, self.offset_v.items[i], .big);
        }
        try bw.writeInt(u16, @intCast(offset_len), .big);

        return try allocator.dupe(u8, buf.items);
    }

    pub fn decode(allocator: std.mem.Allocator, data: []const u8) !Block {
        const e_num_of_elements = data[data.len - @sizeOf(u16) ..];
        const num_of_elements = asInt(u16, e_num_of_elements);
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

pub const BlockIteratorPtr = smart_pointer.SmartPointer(BlockIterator);

pub const BlockIterator = struct {
    allocator: std.mem.Allocator,
    block: BlockPtr,
    first_key: []u8,
    key_v: std.ArrayList(u8),
    value_v: std.ArrayList(u8),
    idx: usize,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, block: BlockPtr) !Self {
        return Self{
            .allocator = allocator,
            .block = block,
            .first_key = try block.get().getFirstKey(allocator),
            .key_v = std.ArrayList(u8).init(allocator),
            .value_v = std.ArrayList(u8).init(allocator),
            .idx = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.first_key);
        self.key_v.deinit();
        self.value_v.deinit();
        self.block.release();
    }

    pub fn createAndSeekToFirst(allocator: std.mem.Allocator, block: BlockPtr) !Self {
        var it = try Self.init(allocator, block);
        try it.seekToFirst();
        return it;
    }

    pub fn createAndSeekToKey(allocator: std.mem.Allocator, block: BlockPtr, kk: []const u8) !Self {
        var it = try Self.init(allocator, block);
        try it.seekToKey(kk);
        return it;
    }

    pub fn key(self: Self) []const u8 {
        return self.key_v.items;
    }

    pub fn value(self: Self) []const u8 {
        return self.value_v.items;
    }

    pub fn isEmpty(self: Self) bool {
        return self.key_v.items.len == 0;
    }

    pub fn next(self: *Self) !void {
        self.idx += 1;
        try self.seekTo(self.idx);
    }

    fn seekToFirst(self: *Self) !void {
        try self.seekTo(0);
    }

    fn seekTo(self: *Self, idx: usize) !void {
        if (idx >= self.block.get().offset_v.items.len) {
            self.key_v.clearRetainingCapacity();
            self.value_v.clearRetainingCapacity();
            return;
        }
        const offset: usize = @intCast(self.block.get().offset_v.items[idx]);
        try self.seekToOffset(offset);
        self.idx = idx;
    }

    fn seekToOffset(self: *Self, offset: usize) !void {
        var stream = std.io.fixedBufferStream(self.block.get().data_v.items[offset..]);
        var reader = stream.reader();

        const overlap_len = try reader.readInt(u16, .big);
        const key_len = try reader.readInt(u16, .big);
        const kb = try self.allocator.alloc(u8, key_len);
        defer self.allocator.free(kb);
        _ = try reader.read(kb);
        self.key_v.clearRetainingCapacity();

        const kw = self.key_v.writer();

        _ = try kw.write(self.first_key[0..overlap_len]);
        _ = try kw.write(kb);

        const value_len = try reader.readInt(u16, .big);
        const vb = try self.allocator.alloc(u8, value_len);
        defer self.allocator.free(vb);
        _ = try reader.read(vb);
        self.value_v.clearRetainingCapacity();
        _ = try self.value_v.writer().write(vb);
    }

    fn seekToKey(self: *Self, kk: []const u8) !void {
        var low: usize = 0;
        var high = self.block.get().offset_v.items.len;

        while (low < high) {
            const mid = low + (high - low) / 2;
            try self.seekTo(mid);
            std.debug.assert(!self.isEmpty());
            switch (std.mem.order(u8, self.key(), kk)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return,
            }
        }
        try self.seekTo(low);
    }
};

test "block" {
    var bb = BlockBuilder.init(std.testing.allocator, 4096);
    defer bb.deinit();
    try std.testing.expect(try bb.add("foo1", "bar1"));
    try std.testing.expect(try bb.add("foo2", "bar2"));
    try std.testing.expect(try bb.add("foo3", "bar3"));
    try std.testing.expect(try bb.add("foo4", "bar4"));
    try std.testing.expect(try bb.add("foo5", "bar5"));

    var b = try bb.build();
    defer b.deinit();

    const eb = try b.encode(std.testing.allocator);
    defer std.testing.allocator.free(eb);

    var b2 = try Block.decode(std.testing.allocator, eb);
    defer b2.deinit();

    try std.testing.expectEqual(b.offset_v.items.len, b2.offset_v.items.len);
    for (0..b.offset_v.items.len) |i| {
        try std.testing.expectEqual(b.offset_v.items[i], b2.offset_v.items[i]);
    }
    try std.testing.expectEqual(b.data_v.items.len, b2.data_v.items.len);
    for (0..b.data_v.items.len) |i| {
        try std.testing.expectEqual(b.data_v.items[i], b2.data_v.items[i]);
    }
}

test "block iterator" {
    var bb = BlockBuilder.init(std.testing.allocator, 4096);
    defer bb.deinit();
    try std.testing.expect(try bb.add("foo1", "bar1"));
    try std.testing.expect(try bb.add("foo2", "bar2"));
    try std.testing.expect(try bb.add("foo3", "bar3"));
    try std.testing.expect(try bb.add("foo4", "bar4"));
    try std.testing.expect(try bb.add("foo5", "bar5"));

    var bp = try BlockPtr.create(std.testing.allocator, try bb.build());
    defer bp.release();
    var b_it = try BlockIterator.createAndSeekToFirst(std.testing.allocator, bp.clone());
    defer b_it.deinit();

    try b_it.seekToFirst();
    try std.testing.expectEqualStrings("foo1", b_it.key());
    try std.testing.expectEqualStrings("bar1", b_it.value());

    while (!b_it.isEmpty()) {
        std.debug.print("key: {s}, value: {s}\n", .{ b_it.key(), b_it.value() });
        try b_it.next();
    }

    try b_it.seekToKey("foo3");

    try std.testing.expectEqualStrings("foo3", b_it.key());
    try std.testing.expectEqualStrings("bar3", b_it.value());

    var b_it2 = try BlockIterator.createAndSeekToKey(std.testing.allocator, bp.clone(), "foo3");
    defer b_it2.deinit();
    try std.testing.expectEqualStrings("foo3", b_it2.key());
    try std.testing.expectEqualStrings("bar3", b_it2.value());

    while (!b_it2.isEmpty()) {
        std.debug.print("key: {s}, value: {s}\n", .{ b_it2.key(), b_it2.value() });
        try b_it2.next();
    }
}
