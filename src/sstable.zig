const std = @import("std");
const FileObject = @import("FileObject.zig");
const BloomFilter = @import("bloom_filter/BloomFilter.zig");
const hash = std.hash;

pub const TableMeta = struct {
    allocator: std.mem.Allocator,
    offset: usize,
    first_key: []const u8,
    last_key: []const u8,

    const Self = @This();

    pub fn decodeTableMeta(
        buf: []const u8,
        allocator: std.mem.Allocator,
    ) ![]TableMeta {
        var container = std.ArrayList(TableMeta).init(allocator);
        errdefer container.deinit();
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
        return container.toOwnedSlice();
    }

    pub fn deinit(self: Self) void {
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
    }
};

pub fn batchEncodeTableMeta(metas: []const TableMeta, allocator: std.mem.Allocator) ![]u8 {
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

    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

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
        &buf,
        estimated_size,
        metas,
    ) catch |err| {
        std.debug.panic("encode table meta failed: {s}", .{@errorName(err)});
    };

    return buf.toOwnedSlice();
}

// pub const SsTable = struct {
//     allocator: std.mem.Allocator,
//     file: FileObject,
//     table_meta: TableMeta,
//     meta_offset: usize,
//     bloom: ?BloomFilter,
//     id: u64,
//     first_key: []const u8,
//     last_key: []const u8,
//     max_ts: u64,

//     const Self = @This();

//     fn asInt(comptime T: type, bytes: []const u8) T {
//         return std.mem.readInt(T, bytes[0..@sizeOf(T)], .big);
//     }

//     pub fn open(allocator: std.mem.Allocator, id: u64, file: FileObject) !Self {
//         const len = file.size;

//         // read bloom filter
//         var raw_bloom_offset: [4]u8 = undefined;
//         _ = try file.read(len - 4, raw_bloom_offset[0..]);
//         const bloom_offset: u64 = @intCast(asInt(u32, raw_bloom_offset[0..]));

//         std.debug.assert(len - 4 - bloom_offset > 0);
//         const raw_bloom = try allocator.alloc(u8, len - 4 - bloom_offset);
//         defer allocator.free(raw_bloom);
//         _ = try file.read(bloom_offset, raw_bloom);
//         const bloom_filter = try BloomFilter.decode(allocator, raw_bloom);

//         // read meta
//         var row_meta_offset: [4]u8 = undefined;
//         _ = try file.read(bloom_offset - 4, undefined);
//         const meta_offset: u64 = @intCast(asInt(u32, row_meta_offset[0..]));
//         const raw_meta = try allocator.alloc(u8, bloom_offset - 4 - meta_offset);
//         defer allocator.free(raw_meta);
//         _ = try file.read(meta_offset, raw_meta);
//     }
// };

test "table_meta" {
    const tms = [_]TableMeta{
        .{ .allocator = std.testing.allocator, .offset = 100, .first_key = "abc", .last_key = "def" },
        .{ .allocator = std.testing.allocator, .offset = 200, .first_key = "ghi", .last_key = "jkl" },
        .{ .allocator = std.testing.allocator, .offset = 300, .first_key = "mno", .last_key = "pqr" },
        .{ .allocator = std.testing.allocator, .offset = 400, .first_key = "stu", .last_key = "vwx" },
        .{ .allocator = std.testing.allocator, .offset = 500, .first_key = "yz", .last_key = "ab" },
    };

    const encoded = try batchEncodeTableMeta(tms[0..], std.testing.allocator);
    defer std.testing.allocator.free(encoded);

    const metas = try TableMeta.decodeTableMeta(encoded, std.testing.allocator);
    defer std.testing.allocator.free(metas);

    for (metas) |tm| {
        std.debug.print("{s} {s}\n", .{ tm.first_key, tm.last_key });
        var tmm = tm;
        tmm.deinit();
    }
}
