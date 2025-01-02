const std = @import("std");
const hash = std.hash;

pub const TableMeta = struct {
    offset: usize,
    first_key: []const u8,
    last_key: []const u8,
};

pub fn batchEncodeTableMeta(metas: []const TableMeta, buf: std.ArrayList(u8)) void {
    var estimated_size = @sizeOf(u32);
    for (metas) |meta| {
        estimated_size += @sizeOf(u32);
        estimated_size += @sizeOf(u16);
        estimated_size += meta.first_key.len;
        estimated_size += @sizeOf(u16);
        estimated_size += meta.last_key.len;
    }
    estimated_size += @sizeOf(u32);

    const origin_len = buf.items.len;

    struct {
        fn doWrite(
            _buf: std.ArrayList(u8),
            _estimated_size: usize,
            _metas: []const TableMeta,
        ) !void {
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
        }
    }.doWrite(
        buf,
        estimated_size,
        metas,
    ) catch |err| {
        std.debug.panic("encode table meta failed: {s}", .{@errorName(err)});
    };
}
