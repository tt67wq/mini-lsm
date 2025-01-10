const std = @import("std");
const atomic = std.atomic;
const skiplist = @import("skiplist.zig");
const Wal = @import("Wal.zig");
const RwLock = std.Thread.RwLock;

const Self = @This();
const Map = skiplist.SkipList([]const u8, []const u8);
const GC = std.ArrayList([]const u8);
pub const Bound = Map.Bound;
pub const MemTableIterator = struct {
    iter: Map.Iterator,

    pub fn init(m: Map.Iterator) MemTableIterator {
        return MemTableIterator{
            .iter = m,
        };
    }

    pub fn isEmpty(self: MemTableIterator) bool {
        return self.iter.isEmpty();
    }

    pub fn next(self: *MemTableIterator) void {
        self.iter.next();
    }

    pub fn key(self: MemTableIterator) []const u8 {
        return self.iter.key();
    }

    pub fn value(self: MemTableIterator) ?[]const u8 {
        return self.iter.value();
    }
};

const max_key = "Ω";

map: Map,
lock: RwLock,
wal: ?Wal,
id: usize,
allocator: std.mem.Allocator,
arena: std.heap.ArenaAllocator,
// gabbage: GC,
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

pub fn init(id: usize, allocator: std.mem.Allocator, path: ?[]const u8) Self {
    var rng = std.rand.DefaultPrng.init(0);
    const arena = std.heap.ArenaAllocator.init(allocator);

    return Self{
        .map = Map.init(
            allocator,
            rng.random(),
            compFunc,
            equalFunc,
        ),
        .wal = walInit(path),
        .id = id,
        .allocator = allocator,
        .arena = arena,
        .lock = .{},
    };
}

fn walInit(path: ?[]const u8) ?Wal {
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
    self.arena.deinit();
}

pub fn recoverFromWal(self: *Self) !void {
    const replyer = struct {
        fn doReply(data: []const u8, mm: *Self) !void {
            var stream = std.io.fixedBufferStream(data);
            var reader = stream.reader();

            while (true) {
                const klen = reader.readInt(u32, .big) catch |err| {
                    switch (err) {
                        error.EndOfStream => {
                            return;
                        },
                        else => return err,
                    }
                };
                const allocator = mm.arena.allocator();

                const kbuf = try allocator.alloc(u8, klen);
                _ = try reader.read(kbuf);
                const vlen = try reader.readInt(u32, .big);
                const vbuf = try allocator.alloc(u8, vlen);
                _ = try reader.read(vbuf);

                try mm.putToList(kbuf, vbuf);
            }
        }
        pub fn reply(log: ?*const anyopaque, size: usize, mm_ptr: ?*anyopaque) callconv(.C) usize {
            var content: []const u8 = undefined;
            content.ptr = @ptrCast(log.?);
            const data = content[0..size];

            const mm: *Self = @ptrCast(@alignCast(mm_ptr.?));
            doReply(data, mm) catch |err| {
                std.log.err("failed to reply: {s}", .{@errorName(err)});
                @panic("failed to reply");
            };

            return 0;
        }
    };

    if (self.wal) |_| {
        try self.wal.?.replay(0, -1, replyer.reply, @ptrCast(self));
    }
}

fn putToList(self: *Self, key: []const u8, value: []const u8) !void {
    const kk = try self.arena.allocator().dupe(u8, key);
    const vv = try self.arena.allocator().dupe(u8, value);
    {
        self.lock.lock();
        defer self.lock.unlock();
        try self.map.insert(kk, vv);
    }

    _ = self.approximate_size.fetchAdd(@intCast(key.len + value.len), .monotonic);
}

fn putToWal(self: *Self, key: []const u8, value: []const u8) !void {
    // [key-size: 4bytes][key][value-size: 4bytes][value]

    if (self.wal) |w| {
        var buf = std.ArrayList(u8).init(self.arena.allocator());

        var bw = buf.writer();
        try bw.writeInt(u32, @intCast(key.len), .big);
        _ = try bw.write(key);
        try bw.writeInt(u32, @intCast(value.len), .big);
        _ = try bw.write(value);
        try w.append(buf.items);
    }
}

pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
    try self.putToWal(key, value);
    try self.putToList(key, value);
}

pub fn get(self: *Self, key: []const u8, val: *[]const u8) !bool {
    self.lock.lockShared();
    defer self.lock.unlockShared();
    var vv: []const u8 = undefined;
    if (try self.map.get(key, &vv)) {
        val.* = vv;
        return true;
    }
    return false;
}

pub fn syncWal(self: Self) !void {
    if (self.wal) |w| {
        try w.sync();
    }
}

pub fn isEmpty(self: Self) bool {
    return self.map.isEmpty();
}

pub fn getApproximateSize(self: Self) usize {
    return self.approximate_size.load(.monotonic);
}

// get a iterator over range (lower_bound, upper_bound)
pub fn scan(self: *Self, lower_bound: Bound, upper_bound: Bound) MemTableIterator {
    return MemTableIterator.init(self.map.scan(lower_bound, upper_bound));
}

test "put/get" {
    const allocator = std.testing.allocator;
    defer std.fs.cwd().deleteTree("./tmp/test.mm") catch unreachable;
    var mm = Self.init(0, allocator, "./tmp/test.mm");
    defer mm.deinit();
    for (0..1000) |i| {
        var kb: [64]u8 = undefined;
        var vb: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&kb, "key{:0>5}", .{i});
        const value = try std.fmt.bufPrint(&vb, "value{:0>5}", .{i});
        try mm.put(key, value);
    }
    var val: []const u8 = undefined;
    if (try mm.get("key00000", &val)) {
        try std.testing.expectEqualStrings("value00000", val);
    } else {
        unreachable;
    }
}

test "recover" {
    const allocator = std.testing.allocator;
    defer std.fs.cwd().deleteTree("./tmp/recover.mm") catch unreachable;
    var mm = Self.init(0, allocator, "./tmp/recover.mm");
    try mm.put("foo", "bar");
    try mm.put("foo1", "bar1");
    try mm.put("foo2", "bar2");
    try mm.syncWal();

    mm.deinit();

    // reopen
    mm = Self.init(0, allocator, "./tmp/recover.mm");
    defer mm.deinit();
    try mm.recoverFromWal();

    var val: []const u8 = undefined;
    if (try mm.get("foo", &val)) {
        try std.testing.expectEqualStrings("bar", val);
    } else {
        unreachable;
    }
}

test "scan" {
    const allocator = std.testing.allocator;
    defer std.fs.cwd().deleteTree("./tmp/iter.mm") catch unreachable;
    var mm = Self.init(0, allocator, "./tmp/iter.mm");
    defer mm.deinit();
    try mm.put("a", "a");
    try mm.put("b", "b");
    try mm.put("c", "c");
    try mm.put("d", "d");
    try mm.put("e", "e");

    var it = mm.scan(Bound.init("a", .included), Bound.init("d", .excluded));
    while (!it.isEmpty()) {
        std.debug.print("key: {s}, val: {s}\n", .{ it.key(), it.value().? });
        it.next();
    }
    std.debug.print("======================\n", .{});

    it = mm.scan(Bound.init("c", .excluded), Bound.init("", .unbounded));
    while (!it.isEmpty()) {
        std.debug.print("key: {s}, val: {s}\n", .{ it.key(), it.value().? });
        it.next();
    }
    std.debug.print("======================\n", .{});

    it = mm.scan(Bound.init("", .unbounded), Bound.init("", .unbounded));
    while (!it.isEmpty()) {
        std.debug.print("key: {s}, val: {s}\n", .{ it.key(), it.value().? });
        it.next();
    }
}
