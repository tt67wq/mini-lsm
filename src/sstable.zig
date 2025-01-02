const std = @import("std");
const hash = std.hash;

pub const TableMeta = struct {
    allocator: std.mem.Allocator,
    offset: usize,
    first_key: []const u8,
    last_key: []const u8,

    const Self = @This();

    pub fn decodeTableMeta(buf: []const u8, allocator: std.mem.Allocator, container: *std.ArrayList(TableMeta)) !void {
        var stream = std.io.fixedBufferStream(buf);
        var reader = stream.reader();
        const num = try reader.readInt(u32, .big);
        const cksm = hash.Crc32.hash(buf[4..(buf.len - 4)]);
        for (0..num) |_| {
            const offset = try reader.readInt(u32, .big);
            const first_key_len = try reader.readInt(u16, .big);
            const first_key = try allocator.alloc(u8, first_key_len);
            errdefer allocator.free(first_key);
            _ = try reader.readAll(first_key);
            const last_key_len = try reader.readInt(u16, .big);
            const last_key = try allocator.alloc(u8, last_key_len);
            errdefer allocator.free(last_key);
            _ = try reader.readAll(last_key);
            try container.append(TableMeta{
                .allocator = allocator,
                .offset = offset,
                .first_key = first_key,
                .last_key = last_key,
            });
        }
        const cksm2 = try reader.readInt(u32, .big);
        if (cksm2 != cksm) {
            std.debug.panic("checksum mismatch", .{});
        }
    }

    pub fn deinit(self: Self) void {
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
    }
};

pub fn batchEncodeTableMeta(metas: []const TableMeta, buf: *std.ArrayList(u8)) void {
    var estimated_size: usize = 0;
    estimated_size += @sizeOf(u32);
    for (metas) |meta| {
        estimated_size += @sizeOf(u32);
        estimated_size += @sizeOf(u16);
        estimated_size += meta.first_key.len;
        estimated_size += @sizeOf(u16);
        estimated_size += meta.last_key.len;
    }
    estimated_size += @sizeOf(u32);

    const writeFn = struct {
        fn doWrite(
            _buf: *std.ArrayList(u8),
            _estimated_size: usize,
            _metas: []const TableMeta,
        ) !void {
            const origin_len = _buf.items.len;
            try _buf.ensureUnusedCapacity(_estimated_size);
            var buf_writer = _buf.writer();
            try buf_writer.writeInt(u32, @intCast(_metas.len), .big);
            for (_metas) |meta| {
                try buf_writer.writeInt(u32, @intCast(meta.offset), .big);
                try buf_writer.writeInt(u16, @intCast(meta.first_key.len), .big);
                try buf_writer.writeAll(meta.first_key);
                try buf_writer.writeInt(u16, @intCast(meta.last_key.len), .big);
                try buf_writer.writeAll(meta.last_key);
            }
            try buf_writer.writeInt(u32, hash.Crc32.hash(_buf.items[(origin_len + 4)..]), .big);
            std.debug.assert(_estimated_size == _buf.items.len - origin_len);
        }
    }.doWrite;

    writeFn(
        buf,
        estimated_size,
        metas,
    ) catch |err| {
        std.debug.panic("encode table meta failed: {s}", .{@errorName(err)});
    };
}

test "table_meta" {
    const tms = [_]TableMeta{
        .{ .allocator = std.testing.allocator, .offset = 100, .first_key = "abc", .last_key = "def" },
        .{ .allocator = std.testing.allocator, .offset = 200, .first_key = "ghi", .last_key = "jkl" },
        .{ .allocator = std.testing.allocator, .offset = 300, .first_key = "mno", .last_key = "pqr" },
        .{ .allocator = std.testing.allocator, .offset = 400, .first_key = "stu", .last_key = "vwx" },
        .{ .allocator = std.testing.allocator, .offset = 500, .first_key = "yz", .last_key = "ab" },
    };

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    batchEncodeTableMeta(tms[0..], &buf);

    var container = std.ArrayList(TableMeta).init(std.testing.allocator);
    defer container.deinit();

    try TableMeta.decodeTableMeta(buf.items, std.testing.allocator, &container);

    for (container.items) |tm| {
        std.debug.print("{s} {s}\n", .{ tm.first_key, tm.last_key });
        var tmm = tm;
        tmm.deinit();
    }
}
