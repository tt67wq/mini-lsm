const std = @import("std");
const expect = std.testing.expect;
const Allocator = std.mem.Allocator;

const BitArrayError = error{
    UnsupportedArraySize,
    InvalidBitOffset,
};

allocator: Allocator,
/// Slice of bytes used as bit store.
bytes: []u8,
/// Length in bits of the bit array.
len: u64,

const Self = @This();

/// Initialize new BitArray given allocator and length in bits of BitArray.
pub fn init(allocator: Allocator, num_bits: u64) !Self {
    // determine number of bytes of memory needed
    const num_bytes: u64 = if (num_bits % 8 > 0) (num_bits / 8) + 1 else (num_bits / 8);

    // check requested bit array size is supported by machine arch
    if (num_bytes > std.math.maxInt(usize)) return BitArrayError.UnsupportedArraySize;
    const bytes = try allocator.alloc(u8, num_bytes);
    @memset(bytes, 0);

    return .{
        .allocator = allocator,
        .bytes = bytes,
        .len = num_bits,
    };
}

pub fn clear(self: *Self) !void {
    self.allocator.free(self.bytes);

    const bytes = try self.allocator.alloc(u8, self.len);
    @memset(bytes, 0);
    self.bytes = bytes;
}

pub fn encode(self: Self, allocator: Allocator) ![]u8 {
    var s = std.ArrayList(u8).init(allocator);
    var writer = s.writer();
    try writer.writeInt(u64, self.len, .big);
    try writer.writeAll(self.bytes);
    return s.toOwnedSlice();
}

pub fn decode(allocator: Allocator, encoded: []u8) !Self {
    var s = std.io.fixedBufferStream(encoded);
    var reader = s.reader();

    const len = try reader.readInt(u64, .big);
    const bytes = try reader.readAllAlloc(allocator, std.math.maxInt(usize));
    return .{
        .allocator = allocator,
        .bytes = bytes,
        .len = len,
    };
}

/// Deinitialize and free resources
pub fn deinit(self: *Self) void {
    self.allocator.free(self.bytes);
}

/// Get value of bit.
pub fn getBit(self: Self, idx: u64) BitArrayError!u1 {
    try self.isValidBitIdx(idx);
    const offset = bitOffset(idx);
    return @truncate(
        ((self.bytes[byteIdx(idx)] & (@as(u8, 1) << offset))) >> offset,
    );
}

/// Set value of bit to 1.
pub fn setBit(self: *Self, idx: u64) BitArrayError!void {
    try self.isValidBitIdx(idx);
    const i = byteIdx(idx);
    self.bytes[i] |= @as(u8, 1) << bitOffset(idx);
}

/// Clear value of bit to 0.
pub fn clearBit(self: *Self, idx: u64) BitArrayError!void {
    try self.isValidBitIdx(idx);
    self.bytes[byteIdx(idx)] &= ~(@as(u8, 1) << bitOffset(idx));
}

/// Toggle value of bit.
pub fn toggleBit(self: *Self, idx: u64) BitArrayError!void {
    try self.isValidBitIdx(idx);
    self.bytes[byteIdx(idx)] ^= @as(u8, 1) << bitOffset(idx);
}

fn isValidBitIdx(self: Self, idx: u64) BitArrayError!void {
    if (idx >= self.len) return BitArrayError.InvalidBitOffset;
}

fn byteIdx(bit_idx: u64) usize {
    const byte_idx: u64 = bit_idx / 8;

    // @truncate of byte_idx from u64 to usize will never truncate any
    // significant as we always check that bit_idx < the length of the bit
    // array, whose size in bytes is constrained at initialization to be always
    // addressable by a usize pointer.
    return @truncate(byte_idx);
}

fn bitOffset(bit_idx: u64) u3 {
    return @truncate(bit_idx % 8);
}

test "init_deinit" {
    var bits = try Self.init(std.testing.allocator, 1000);
    defer bits.deinit();
}

test "get" {
    var bits = try Self.init(std.testing.allocator, 1000);
    defer bits.deinit();

    try expect(0 == try bits.getBit(200));
}

test "set" {
    var bits = try Self.init(std.testing.allocator, 208);
    defer bits.deinit();

    try bits.setBit(168);
    try expect(1 == try bits.getBit(168));
}

test "clear" {
    var bits = try Self.init(std.testing.allocator, 1000);
    defer bits.deinit();

    try bits.setBit(400);
    try bits.clearBit(400);
    try expect(0 == try bits.getBit(400));
}

test "toggle" {
    var bits = try Self.init(std.testing.allocator, 1000);
    defer bits.deinit();

    try bits.toggleBit(400);
    try expect(1 == try bits.getBit(400));
}
