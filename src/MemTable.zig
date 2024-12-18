const std = @import("std");
const skiplist = @import("skiplist.zig");
const Wal = @import("Wal.zig");

const Self = @This();
const MemtableError = error{
    NotFound,
};

const max_key = "Ω";

map: *skiplist.SkipList([]const u8, []const u8),
wal: Wal,
id: usize,
approximate_size: usize,
allocator: std.mem.Allocator,

fn compFunc(a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, b, max_key)) {
        return true;
    }
    return std.mem.lessThan(u8, a, b);
}

fn equalFunc(a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, b, max_key)) {
        return false;
    }
    return std.mem.eql(u8, a, b);
}

pub fn init(id: usize, allocator: std.mem.Allocator, path: []const u8) Self {
    var rng = std.rand.DefaultPrng.init(0);
    var list = skiplist.SkipList([]const u8, []const u8).init(
        allocator,
        rng.random(),
        compFunc,
    );
    return Self{
        .map = &list,
        .wal = Wal.init(path) catch |err| {
            std.log.err("failed to create wal: {s}", .{@errorName(err)});
            @panic("failed to create wal");
        },
        .id = id,
        .approximate_size = 0,
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self) void {
    self.map.deinit();
    self.wal.deinit();
}

fn serializeInteger(comptime T: type, i: T, buf: *[@sizeOf(T)]u8) void {
    std.mem.writeInt(T, buf, i, .big);
}

fn deserializeInteger(comptime T: type, buf: []const u8) T {
    return std.mem.readInt(T, buf[0..@sizeOf(T)], .big);
}

// [length: 4bytes][bytes]
pub fn serializeBytes(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var h: [4]u8 = undefined;
    serializeInteger(u32, @intCast(bytes.len), &h);
    const parts = [_][]const u8{ &h, bytes };
    return std.mem.concat(allocator, u8, &parts);
}

pub fn deserializeBytes(bytes: []const u8, buf: *[]const u8) usize {
    const length = deserializeInteger(u32, bytes);
    const offset = length + 4;
    buf.* = bytes[4..offset];
    return offset;
}

pub fn put(self: Self, key: []const u8, value: []const u8) !void {
    const k = try serializeBytes(self.allocator, key);
    defer self.allocator.free(k);
    const v = try serializeBytes(self.allocator, value);
    defer self.allocator.free(v);
    var id: [4]u8 = undefined;
    serializeInteger(u32, @intCast(self.id), &id);
    var buf = std.ArrayList(u8).init(self.allocator);
    try buf.appendSlice(id[0..4]);
    try buf.appendSlice(k);
    try buf.appendSlice(v);
    try self.wal.append(try buf.toOwnedSlice());

    const kk = try self.allocator.dupeZ(u8, key);
    const vv = try self.allocator.dupeZ(u8, value);
    try self.map.insert(kk, vv);
}

pub fn get(self: Self, key: []const u8, val: *[]const u8) !void {
    const v = self.map.get(key, equalFunc);
    if (v) |vv| {
        val.* = vv;
    } else {
        return MemtableError.NotFound;
    }
}

test "put" {
    const allocator = std.testing.allocator;
    var mm = Self.init(0, allocator, "./tmp/test.mm");
    defer mm.deinit();
    try mm.put("foo", "bar");
    var val: []const u8 = undefined;
    try mm.get("foo", &val);
    try std.testing.expectEqualStrings("bar", val);
}
