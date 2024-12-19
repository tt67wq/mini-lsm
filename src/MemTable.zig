const std = @import("std");
const skiplist = @import("skiplist.zig");
const Wal = @import("Wal.zig");

const Self = @This();
const MemtableError = error{
    NotFound,
};
const Map = skiplist.SkipList([]const u8, []const u8);
const GC = std.ArrayList([]const u8);

const max_key = "Ω";

map: *Map,
wal: Wal,
id: usize,
allocator: std.mem.Allocator,
gabbage: *GC,

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

pub fn init(id: usize, allocator: std.mem.Allocator, path: []const u8) !Self {
    var rng = std.rand.DefaultPrng.init(0);
    const map = try allocator.create(Map);
    map.* = skiplist.SkipList([]const u8, []const u8).init(
        allocator,
        rng.random(),
        compFunc,
        equalFunc,
    );
    const gc: *GC = try allocator.create(GC);
    gc.* = std.ArrayList([]const u8).init(allocator);
    return Self{
        .map = map,
        .wal = Wal.init(path) catch |err| {
            std.log.err("failed to create wal: {s}", .{@errorName(err)});
            @panic("failed to create wal");
        },
        .id = id,
        .allocator = allocator,
        .gabbage = gc,
    };
}

pub fn deinit(self: *Self) void {
    self.map.deinit();
    self.allocator.destroy(self.map);
    self.wal.deinit();

    // free gabbage
    for (self.gabbage.items) |item| {
        self.allocator.free(item);
    }
    self.gabbage.deinit();
    self.allocator.destroy(self.gabbage);
}

fn serializeInteger(comptime T: type, i: T, buf: *[@sizeOf(T)]u8) void {
    std.mem.writeInt(T, buf, i, .big);
}

fn deserializeInteger(comptime T: type, buf: []const u8) T {
    return std.mem.readInt(T, buf[0..@sizeOf(T)], .big);
}

// [length: 4bytes][bytes]
pub fn serializeBytes(bytes: []const u8, buf: *std.ArrayList(u8)) !void {
    var h: [4]u8 = undefined;
    serializeInteger(u32, @intCast(bytes.len), &h);
    try buf.appendSlice(h[0..4]);
    try buf.appendSlice(bytes);
}

pub fn deserializeBytes(bytes: []const u8, buf: *[]const u8) usize {
    const length = deserializeInteger(u32, bytes);
    const offset = length + 4;
    buf.* = bytes[4..offset];
    return offset;
}

pub fn put(self: Self, key: []const u8, value: []const u8) !void {
    var id: [4]u8 = undefined;
    serializeInteger(u32, @intCast(self.id), &id);
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    try buf.appendSlice(id[0..4]);

    try serializeBytes(key, &buf);
    try serializeBytes(value, &buf);
    try self.wal.append(buf.items);

    const kk = try self.allocator.dupe(u8, key);
    errdefer self.allocator.free(kk);
    const vv = try self.allocator.dupe(u8, value);
    errdefer self.allocator.free(vv);
    try self.map.insert(kk, vv);
    try self.gabbage.append(kk);
    try self.gabbage.append(vv);
}

fn may_gc(self: Self) void {
    if (self.gabbage.items.len > 4096) {
        for (self.gabbage.items) |item| {
            self.allocator.free(item);
        }
        self.gabbage.clearRetainingCapacity();
    }
}

pub fn get(self: Self, key: []const u8, val: *[]const u8) !void {
    const v = self.map.get(key);
    if (v) |vv| {
        val.* = vv;
    } else {
        return MemtableError.NotFound;
    }
}

pub fn sync_wal(self: Self) !void {
    try self.wal.sync();
}

pub fn is_empty(self: Self) bool {
    return self.map.is_empty();
}

test "put/get" {
    const allocator = std.testing.allocator;
    var mm = try Self.init(0, allocator, "./tmp/test.mm");
    defer mm.deinit();
    try mm.put("foo", "bar");
    var val: []const u8 = undefined;
    try mm.get("foo", &val);
    try std.testing.expectEqualStrings("bar", val);
}
