const std = @import("std");
const io = std.io;
const expect = std.testing.expect;
const math = std.math;
const Allocator = std.mem.Allocator;
const BitArray = @import("BitArray.zig");
const Murmur = std.hash.Murmur3_32;

const BloomFilterError = error{
    UnsupportedSpec,
};

/// Bloom filter with allocation of bit array at runtime initialization. Uses
/// Murmur3 hash functions.
num_hash_funcs: u8,
bits: BitArray,

const Self = @This();

/// Initialize a new Bloom filter that is configured and sized to hold
/// a specified maximum number of items with a given false positive rate.
pub fn init(
    allocator: std.mem.Allocator,
    max_items: u64,
    fp_rate: f32,
) !Self {
    const m = calcM(max_items, fp_rate);
    const k = calcK(max_items, m);

    // check `k` is < 256
    if (k > std.math.maxInt(u8)) {
        return BloomFilterError.UnsupportedSpec;
    }

    return .{
        .num_hash_funcs = @truncate(k),
        .bits = try BitArray.init(allocator, m),
    };
}

/// Deinitialize and free resources
pub fn deinit(self: *Self) void {
    self.bits.deinit();
}

pub fn clear(self: *Self) !void {
    try self.bits.clear();
}

pub fn encode(self: Self, allocator: std.mem.Allocator) ![]u8 {
    var s = std.ArrayList(u8).init(allocator);
    var writer = s.writer();
    try writer.writeByte(self.num_hash_funcs);
    const bytes = try self.bits.encode(allocator);
    defer allocator.free(bytes);
    try writer.writeAll(bytes);
    return s.toOwnedSlice();
}

pub fn decode(allocator: std.mem.Allocator, encoded: []u8) !Self {
    var s = std.io.fixedBufferStream(encoded);
    var reader = s.reader();
    const num_hash_funcs = try reader.readByte();
    const bits = try BitArray.decode(allocator, encoded[1..]);
    return .{
        .num_hash_funcs = num_hash_funcs,
        .bits = bits,
    };
}

/// Insert an item into the Bloom filter.
pub fn insert(self: *Self, item: []const u8) !void {
    for (0..self.num_hash_funcs) |i| {
        try self.bits.setBit(self.calcBitIdx(item, @truncate(i)));
    }
}

/// Returns whether Bloom filter contains the item. It may return a false
/// positive, but will never return a false negative.
pub fn contains(self: Self, item: []const u8) !bool {
    for (0..self.num_hash_funcs) |i| {
        if (try self.bits.getBit(self.calcBitIdx(item, @truncate(i))) == 0) {
            return false;
        }
    }
    return true;
}

/// Calculate index of bit for given item and hashing function seed
fn calcBitIdx(self: Self, item: []const u8, hash_seed: u32) u64 {
    const hash = Murmur.hashWithSeed(item, hash_seed);
    return hash % self.bits.len;
}

/// Calculate the appropriate number in bits of the Bloom filter, `m`, given
/// `n`, the expected number of elements contained in the Bloom filter and the
/// target false positive rate, `f`.
///
/// `(-nln(f))/ln(2)^2`
fn calcM(n: u64, f: f64) u64 {
    const numerator = @as(f64, @floatFromInt(n)) * -math.log(f64, math.e, f);
    const denominator = math.pow(f64, (math.log(f64, math.e, 2)), 2);
    return @intFromFloat(
        math.divTrunc(f64, numerator, denominator) catch unreachable,
    );
}

/// Calculate the number of hash functions to use, `k`, given `n` and `m`, the expected
/// number of elements contained in the Bloom filter and the size in bits of the Bloom
/// filter.
///
/// `(mln(2)/n)`
fn calcK(n: u64, m: u64) u64 {
    // https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
    const numerator = @as(f64, @floatFromInt(m)) * math.log(f64, math.e, 2);
    const denominator = @as(f64, @floatFromInt(n));
    return @as(u64, @intFromFloat(
        math.divTrunc(f64, numerator, denominator) catch unreachable,
    ));
}

test "calcM" {
    const n: usize = 1_000_000;
    const f: f32 = 0.02;
    try expect(calcM(n, f) == 8_142_363);
}

test "calcK" {
    const n: usize = 1_000_000;
    const m: usize = 8_142_363;
    try expect(calcK(n, m) == 5);
}

test "init" {
    var filter = try Self.init(std.testing.allocator, 100_000, 0.02);
    defer filter.deinit();
}

test "contains_true" {
    var filter = try Self.init(std.testing.allocator, 100_000, 0.02);
    defer filter.deinit();

    try filter.insert("hi");
    try expect(try filter.contains("hi") == true);
}

test "contains_false" {
    var filter = try Self.init(std.testing.allocator, 100_000, 0.02);
    defer filter.deinit();

    try filter.insert("hi");
    try expect(try filter.contains("yo") == false);
}

test "encode/decode" {
    var filter = try Self.init(std.testing.allocator, 1024, 0.02);
    defer filter.deinit();

    try filter.insert("foo1");
    try filter.insert("foo2");
    try filter.insert("foo3");
    try filter.insert("foo4");
    try filter.insert("foo5");

    const bytes = try filter.encode(std.testing.allocator);
    defer std.testing.allocator.free(bytes);

    var filter2 = try Self.decode(std.testing.allocator, bytes);
    defer filter2.deinit();
    try expect(try filter2.contains("foo1") == true);
    try expect(try filter2.contains("foo2") == true);
    try expect(try filter2.contains("foo3") == true);
    try expect(try filter2.contains("foo4") == true);
    try expect(try filter2.contains("foo5") == true);
}
