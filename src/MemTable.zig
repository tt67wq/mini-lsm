const std = @import("std");
const skiplist = @import("skiplist.zig");
const Wal = @import("Wal.zig");
const ss_table = @import("ss_table.zig");
const smart_pointer = @import("smart_pointer.zig");
const atomic = std.atomic;
const RwLock = std.Thread.RwLock;
const SsTableBuilder = ss_table.SsTableBuilder;

const Self = @This();

pub const MemTablePtr = smart_pointer.SmartPointer(Self);
pub const Bound = skiplist.Bound;
pub const MemTableIterator = struct {
    iter: skiplist.Iterator,

    pub fn init(m: skiplist.Iterator) MemTableIterator {
        return MemTableIterator{ .iter = m };
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

    pub fn value(self: MemTableIterator) []const u8 {
        return self.iter.value();
    }

    pub fn numActiveIterators(_: MemTableIterator) usize {
        return 1;
    }
};

var rng = std.rand.DefaultPrng.init(0);

skiplist: skiplist,
lock: RwLock,
wal: ?Wal,
id: usize,
allocator: std.mem.Allocator,
approximate_size: atomic.Value(usize) = atomic.Value(usize).init(0),

pub fn init(id: usize, allocator: std.mem.Allocator, path: ?[]const u8) Self {
    return Self{
        .skiplist = skiplist.init(allocator, rng.random()),
        .wal = walInit(path),
        .id = id,
        .allocator = allocator,
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
    self.skiplist.deinit();
    if (self.wal) |_| {
        self.wal.?.deinit();
    }
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
                const allocator = mm.allocator;

                const kbuf = try allocator.alloc(u8, klen);
                defer allocator.free(kbuf);
                _ = try reader.read(kbuf);
                const vlen = try reader.readInt(u32, .big);
                const vbuf = try allocator.alloc(u8, vlen);
                defer allocator.free(vbuf);
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
    {
        self.lock.lock();
        defer self.lock.unlock();
        try self.skiplist.insert(key, value);
    }

    _ = self.approximate_size.fetchAdd(@intCast(key.len + value.len), .monotonic);
}

fn putToWal(self: *Self, key: []const u8, value: []const u8) !void {
    // [key-size: 4bytes][key][value-size: 4bytes][value]

    if (self.wal) |w| {
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();

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
    if (try self.skiplist.get(key, &vv)) {
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
    return self.skiplist.isEmpty();
}

pub fn getApproximateSize(self: Self) usize {
    return self.approximate_size.load(.monotonic);
}

// get a iterator over range (lower_bound, upper_bound)
pub fn scan(self: *Self, lower_bound: Bound, upper_bound: Bound) MemTableIterator {
    return MemTableIterator.init(self.skiplist.scan(lower_bound, upper_bound));
}

pub fn flush(self: Self, builder: *SsTableBuilder) !void {
    var it = self.skiplist.scan(Bound.init("", .unbounded), Bound.init("", .unbounded));
    while (!it.isEmpty()) {
        try builder.add(it.key(), it.value());
        it.next();
    }
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

    for (0..512) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try mm.put(kk, vv);
    }

    var it = mm.scan(Bound.init("key00278", .included), Bound.init("key00299", .excluded));
    while (!it.isEmpty()) {
        std.debug.print("key: {s}, val: {s}\n", .{ it.key(), it.value() });
        it.next();
    }

    it = mm.scan(Bound.init("key00555", .included), Bound.init("", .unbounded));
    while (!it.isEmpty()) {
        std.debug.print("key: {s}, val: {s}\n", .{ it.key(), it.value() });
        it.next();
    }
}
