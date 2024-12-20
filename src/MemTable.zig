const std = @import("std");
const atomic = std.atomic;
const skiplist = @import("skiplist.zig");
const Wal = @import("Wal.zig");
const RwLock = std.Thread.RwLock;

const Self = @This();
const MemtableError = error{
    NotFound,
};
const Map = skiplist.SkipList([]const u8, []const u8);
const GC = std.ArrayList([]const u8);
const MemTableIterator = struct {
    iter: Map.Iterator,

    pub fn init(m: Map.Iterator) MemTableIterator {
        return MemTableIterator{
            .iter = m,
        };
    }

    pub fn hasNext(self: MemTableIterator) bool {
        return self.iter.hasNext();
    }

    pub fn next(self: *MemTableIterator, key: *[]const u8, value: *[]const u8) void {
        const p = self.iter.next().?;
        key.* = p.key;
        if (p.value) |v| {
            value.* = v;
        }
    }
};

const max_key = "Ω";

map: Map,
lock: RwLock,
wal: ?Wal,
id: usize,
allocator: std.mem.Allocator,
gabbage: GC,
approximate_size: atomic.Value(usize) = atomic.Value(usize).init(0),

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

pub fn init(id: usize, allocator: std.mem.Allocator, path: ?[]const u8) !Self {
    var rng = std.rand.DefaultPrng.init(0);

    return Self{
        .map = skiplist.SkipList([]const u8, []const u8).init(
            allocator,
            rng.random(),
            compFunc,
            equalFunc,
        ),
        .wal = wal_init(path),
        .id = id,
        .allocator = allocator,
        .gabbage = std.ArrayList([]const u8).init(allocator),
        .lock = .{},
    };
}

fn wal_init(path: ?[]const u8) ?Wal {
    if (path) |p| {
        return Wal.init(p) catch |err| {
            std.log.err("failed to create wal: {s}", .{@errorName(err)});
            @panic("failed to create wal");
        };
    }
    return null;
}

pub fn deinit(self: *Self) void {
    self.map.deinit();
    if (self.wal) |_| {
        self.wal.?.deinit();
    }

    // free gabbage
    for (self.gabbage.items) |item| {
        self.allocator.free(item);
    }
    self.gabbage.deinit();
}

fn serializeInteger(comptime T: type, i: T, buf: *[@sizeOf(T)]u8) void {
    std.mem.writeInt(T, buf, i, .big);
}

fn deserializeInteger(comptime T: type, buf: []const u8) T {
    return std.mem.readInt(T, buf[0..@sizeOf(T)], .big);
}

// [length: 4bytes][bytes]
fn serializeBytes(bytes: []const u8, buf: *std.ArrayList(u8)) !void {
    var h: [4]u8 = undefined;
    serializeInteger(u32, @intCast(bytes.len), &h);
    try buf.appendSlice(h[0..4]);
    try buf.appendSlice(bytes);
}

fn deserializeBytes(bytes: []const u8, buf: *[]const u8) usize {
    const length = deserializeInteger(u32, bytes);
    const offset = length + 4;
    buf.* = bytes[4..offset];
    return offset;
}

pub fn recover_from_wal(self: *Self) !void {
    const replyer = struct {
        pub fn reply(log: ?*const anyopaque, size: usize, mm_ptr: ?*anyopaque) callconv(.C) usize {
            var content: []const u8 = undefined;
            content.ptr = @ptrCast(log.?);
            const data = content[0..size];
            var offset: usize = 0;
            while (offset < size) {
                var kbuf: []const u8 = undefined;
                var vbuf: []const u8 = undefined;
                offset += deserializeBytes(data[offset..], &kbuf);
                offset += deserializeBytes(data[offset..], &vbuf);

                var mm: *Self = @ptrCast(@alignCast(mm_ptr.?));

                mm.put_to_list(kbuf, vbuf) catch |err| {
                    std.log.err("failed to put to list: {s}", .{@errorName(err)});
                    @panic("failed to put to list");
                };
            }
            return 0;
        }
    };

    if (self.wal) |_| {
        try self.wal.?.replay(0, -1, replyer.reply, @ptrCast(self));
    }
}

fn put_to_list(self: *Self, key: []const u8, value: []const u8) !void {
    const kk = try self.allocator.dupe(u8, key);
    errdefer self.allocator.free(kk);
    const vv = try self.allocator.dupe(u8, value);
    errdefer self.allocator.free(vv);
    if (self.lock.tryLock()) {
        defer self.lock.unlock();
        try self.map.insert(kk, vv);
    }
    try self.gabbage.append(kk);
    try self.gabbage.append(vv);

    _ = self.approximate_size.fetchAdd(@intCast(key.len + value.len), .monotonic);
}

fn put_to_wal(self: Self, key: []const u8, value: []const u8) !void {
    // [key-size: 4bytes][key][value-size: 4bytes][value]
    if (self.wal) |w| {
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        try serializeBytes(key, &buf);
        try serializeBytes(value, &buf);
        try w.append(buf.items);
    }
}

pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
    try self.put_to_wal(key, value);
    try self.put_to_list(key, value);
}

pub fn get(self: *Self, key: []const u8, val: *[]const u8) !void {
    if (self.lock.tryLockShared()) {
        defer self.lock.unlockShared();
        const v = self.map.get(key);
        if (v) |vv| {
            val.* = vv;
        } else {
            return MemtableError.NotFound;
        }
    }
}

pub fn sync_wal(self: Self) !void {
    if (self.wal) |w| {
        try w.sync();
    }
}

pub fn is_empty(self: Self) bool {
    return self.map.is_empty();
}

pub fn get_approximate_size(self: Self) usize {
    return self.approximate_size.load(.monotonic);
}

// get a iterator over range [lower_bound, upper_bound)
pub fn iter(self: *Self, lower_bound: []const u8, upper_bound: []const u8) MemTableIterator {
    return MemTableIterator.init(self.map.iter(lower_bound, upper_bound));
}

test "put/get" {
    const allocator = std.testing.allocator;
    defer std.fs.cwd().deleteTree("./tmp/test.mm") catch unreachable;
    var mm = try Self.init(0, allocator, "./tmp/test.mm");
    defer mm.deinit();
    try mm.put("foo", "bar");
    var val: []const u8 = undefined;
    try mm.get("foo", &val);
    try std.testing.expectEqualStrings("bar", val);
}

test "recover" {
    const allocator = std.testing.allocator;
    defer std.fs.cwd().deleteTree("./tmp/recover.mm") catch unreachable;
    var mm = try Self.init(0, allocator, "./tmp/recover.mm");
    try mm.put("foo", "bar");
    try mm.put("foo1", "bar1");
    try mm.put("foo2", "bar2");
    try mm.sync_wal();

    mm.deinit();

    // reopen
    mm = try Self.init(0, allocator, "./tmp/recover.mm");
    defer mm.deinit();
    try mm.recover_from_wal();

    var val: []const u8 = undefined;
    try mm.get("foo", &val);
    try std.testing.expectEqualStrings("bar", val);
}

test "iter" {
    const allocator = std.testing.allocator;
    defer std.fs.cwd().deleteTree("./tmp/iter.mm") catch unreachable;
    var mm = try Self.init(0, allocator, "./tmp/iter.mm");
    defer mm.deinit();
    try mm.put("foo", "bar");
    try mm.put("foo1", "bar1");
    try mm.put("foo2", "bar2");

    var it = mm.iter("foo", max_key);
    var key: []const u8 = undefined;
    var val: []const u8 = undefined;
    while (it.hasNext()) {
        it.next(&key, &val);
        std.debug.print("key: {s}, val: {s}\n", .{ key, val });
    }
}
