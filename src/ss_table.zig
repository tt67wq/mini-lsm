const std = @import("std");
const FileObject = @import("FileObject.zig");
const BloomFilter = @import("bloom_filter/BloomFilter.zig");
const block = @import("block.zig");
const lru = @import("lru.zig");
const Block = block.Block;
const BlockBuilder = block.BlockBuilder;
const hash = std.hash;

const BlockCache = lru.LruCache(.locking, usize, Block);

pub const SsTableBuilder = struct {
    allocator: std.mem.Allocator,
    builder: BlockBuilder,
    first_key: ?[]const u8,
    last_key: ?[]const u8,
    meta: std.ArrayList(BlockMeta),
    block_size: usize,
    data: std.ArrayList(u8),
    bloom: BloomFilter,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, block_size: usize) !Self {
        return .{
            .allocator = allocator,
            .builder = BlockBuilder.init(allocator, block_size),
            .first_key = null,
            .last_key = null,
            .meta = std.ArrayList(BlockMeta).init(allocator),
            .block_size = block_size,
            .data = std.ArrayList(u8).init(allocator),
            .bloom = try BloomFilter.init(allocator, block_size / 3, 0.01),
        };
    }

    pub fn deinit(self: *Self) void {
        self.builder.deinit();
        for (self.meta.items) |meta| {
            meta.deinit();
        }
        self.meta.deinit();
        self.data.deinit();
        self.bloom.deinit();
        if (self.first_key) |first_key| {
            self.allocator.free(first_key);
        }
        if (self.last_key) |last_key| {
            self.allocator.free(last_key);
        }
    }

    pub fn add(self: *Self, key: []const u8, value: []const u8) !void {
        try self.setFirstKey(key);
        try self.bloom.insert(key);

        if (try self.builder.add(key, value)) {
            try self.setLastKey(key);
            return;
        }
        // block is full
        try self.finishBlock();
        std.debug.assert(try self.builder.add(key, value));
        try self.resetFirstKey(key);
        try self.setLastKey(key);
    }

    fn resetFirstKey(self: *Self, key: []const u8) !void {
        if (self.first_key) |k| {
            self.allocator.free(k);
        }
        self.first_key = try self.allocator.dupe(u8, key);
    }

    fn setFirstKey(self: *Self, key: []const u8) !void {
        if (self.first_key) |_| {} else {
            self.first_key = try self.allocator.dupe(u8, key);
        }
    }

    fn setLastKey(self: *Self, key: []const u8) !void {
        if (self.last_key) |l| self.allocator.free(l);
        self.last_key = try self.allocator.dupe(u8, key);
    }

    // | encoded_block0 | checksum: 4 | encoded_block1 | checksum: 4 | ...... |
    fn finishBlock(self: *Self) !void {
        var bo = self.builder;
        // reset block
        defer bo.reset();

        self.builder = BlockBuilder.init(self.allocator, self.block_size);
        var blk = try bo.build();
        defer blk.deinit();
        const encoded_block = try blk.encode(self.allocator);
        defer self.allocator.free(encoded_block);
        try self.meta.append(.{
            .allocator = self.allocator,
            .offset = self.data.items.len,
            .first_key = try self.allocator.dupe(u8, self.first_key.?),
            .last_key = try self.allocator.dupe(u8, self.last_key.?),
        });
        const cksm = hash.Crc32.hash(encoded_block);
        try self.data.appendSlice(encoded_block);
        try self.data.writer().writeInt(u32, cksm, .big);
    }

    // the tail part of sstable:
    // |......| block_meta_encoded | meta_offset: 4 | bloom_filter_encoded | bloom_offset: 4 |
    pub fn build(
        self: *Self,
        id: usize,
        block_cache: ?BlockCache,
        path: []const u8,
    ) !SsTable {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        try self.finishBlock();
        const w = self.data.writer();
        const meta_offset = self.data.items.len;
        const meta_b = try BlockMeta.batchEncode(self.meta.items, allocator);
        _ = try w.write(meta_b);
        try w.writeInt(u32, @intCast(meta_offset), .big);

        const bloom_offset = self.data.items.len;
        const encoded_bloom = try self.bloom.encode(allocator);
        _ = try w.write(encoded_bloom);
        try w.writeInt(u32, @intCast(bloom_offset), .big);
        const file = try FileObject.init(path, self.data.items);
        errdefer file.deinit();

        var bp = try self.bloom.clone(self.allocator);
        errdefer bp.deinit();

        const fk = self.meta.items[0].first_key;
        const lk = self.meta.getLast().last_key;

        return .{
            .allocator = self.allocator,
            .file = file,
            .block_metas = try self.meta.toOwnedSlice(),
            .meta_offset = meta_offset,
            .block_cache = block_cache,
            .bloom = bp,
            .id = id,
            .first_key = try self.allocator.dupe(u8, fk),
            .last_key = try self.allocator.dupe(u8, lk),
            .max_ts = 0,
        };
    }
};

pub const BlockMeta = struct {
    allocator: std.mem.Allocator,
    offset: usize,
    first_key: []const u8,
    last_key: []const u8,

    const Self = @This();

    pub fn deinit(self: Self) void {
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
    }

    // result is allocated by self.allocator, so caller should free it after use
    pub fn batchDeecode(
        buf: []const u8,
        allocator: std.mem.Allocator,
    ) ![]BlockMeta {
        var container = std.ArrayList(BlockMeta).init(allocator);
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
            try container.append(BlockMeta{
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

    // | estimated_size: 4 | meta_size: 4 | m1_offset: 4 |
    // | m1_first_key_len: 2 | m1_first_key | m1_last_key_len: 2 | m1_last_key | m1_hash: 4|......
    pub fn batchEncode(metas: []const Self, allocator: std.mem.Allocator) ![]u8 {
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
                _metas: []const BlockMeta,
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
};

pub const SsTable = struct {
    allocator: std.mem.Allocator,
    file: FileObject,
    block_metas: []BlockMeta,
    meta_offset: usize,
    block_cache: ?BlockCache,
    bloom: ?BloomFilter,
    id: u64,
    first_key: []const u8,
    last_key: []const u8,
    max_ts: u64,

    const Self = @This();

    const Err = error{
        NotFound,
    };

    fn asInt(comptime T: type, bytes: []const u8) T {
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .big);
    }

    pub fn deinit(self: Self) void {
        self.file.deinit();
        for (self.block_metas) |tm| {
            tm.deinit();
        }
        self.allocator.free(self.block_metas);
        if (self.block_cache) |bc| {
            var bcm = bc;
            bcm.deinit();
        }
        if (self.bloom) |bf| {
            var bfm = bf;
            bfm.deinit();
        }
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
    }

    // | encoded_block0 | checksum: 4 | encoded_block1 | checksum: 4 | ...... | block_meta_encoded | meta_offset: 4 | bloom_filter_encoded | bloom_offset: 4 |
    pub fn open(
        allocator: std.mem.Allocator,
        id: u64,
        file: FileObject,
        block_cache: ?BlockCache,
    ) !Self {
        const len = file.size;

        // read bloom filter
        var raw_bloom_offset: [4]u8 = undefined;
        _ = try file.read(len - 4, raw_bloom_offset[0..]);
        const bloom_offset: u64 = @intCast(asInt(u32, raw_bloom_offset[0..]));

        std.debug.assert(len - 4 - bloom_offset > 0);
        const raw_bloom = try allocator.alloc(u8, len - 4 - bloom_offset);
        defer allocator.free(raw_bloom);
        _ = try file.read(bloom_offset, raw_bloom);
        var bloom_filter = try BloomFilter.decode(allocator, raw_bloom);
        errdefer bloom_filter.deinit();

        // read meta
        var raw_meta_offset: [4]u8 = undefined;
        _ = try file.read(bloom_offset - 4, raw_meta_offset[0..]);
        const meta_offset: u64 = @intCast(asInt(u32, raw_meta_offset[0..]));
        std.debug.assert(bloom_offset - 4 - meta_offset > 0);
        const raw_meta = try allocator.alloc(u8, bloom_offset - 4 - meta_offset);
        defer allocator.free(raw_meta);
        _ = try file.read(meta_offset, raw_meta);
        const tms = try BlockMeta.batchDeecode(raw_meta, allocator);
        errdefer for (tms) |tm| {
            tm.deinit();
        };

        const first_key = try allocator.dupe(u8, tms[0].first_key);
        errdefer allocator.free(first_key);
        const last_key = try allocator.dupe(u8, tms[tms.len - 1].last_key);
        errdefer allocator.free(last_key);

        return .{
            .allocator = allocator,
            .file = file,
            .block_metas = tms,
            .meta_offset = meta_offset,
            .block_cache = block_cache,
            .bloom = bloom_filter,
            .id = id,
            .first_key = first_key,
            .last_key = last_key,
            .max_ts = 0,
        };
    }

    pub fn createMetaOnly(
        allocator: std.mem.Allocator,
        file_size: u64,
        id: u64,
        first_key: []const u8,
        last_key: []const u8,
    ) Self {
        const first_key_d = allocator.dupe(u8, first_key) catch unreachable;
        const last_key_d = allocator.dupe(u8, last_key) catch unreachable;
        return .{
            .allocator = allocator,
            .file = .{
                .file = null,
                .size = file_size,
            },
            .block_metas = [_]BlockMeta{},
            .meta_offset = 0,
            .bloom = null,
            .id = id,
            .first_key = first_key_d,
            .last_key = last_key_d,
            .max_ts = 0,
        };
    }

    pub fn readBlock(self: Self, block_idx: usize) !Block {
        std.debug.assert(block_idx < self.block_metas.len);
        const offset = self.block_metas[block_idx].offset;
        var offset_end: usize = 0;
        if (block_idx + 1 < self.block_metas.len) {
            offset_end = self.block_metas[block_idx + 1].offset;
        } else {
            offset_end = self.meta_offset;
        }
        const block_len = offset_end - offset - 4;
        var block_raw_with_cksm = try self.allocator.alloc(u8, offset_end - offset);
        defer self.allocator.free(block_raw_with_cksm);
        _ = try self.file.read(@intCast(offset), block_raw_with_cksm);
        const block_raw = block_raw_with_cksm[0..block_len];
        const cksum = asInt(u32, block_raw_with_cksm[block_len..]);

        if (hash.Crc32.hash(block_raw) != cksum) {
            std.debug.panic("checksum mismatch", .{});
        }
        return try Block.decode(self.allocator, block_raw);
    }

    pub fn readBlockCached(self: Self, block_idx: usize) !Block {
        if (self.block_cache) |bc| {
            var bcm = bc;
            if (bcm.get(block_idx)) |b| {
                return b;
            } else {
                const b = try self.readBlock(block_idx);
                try bcm.insert(block_idx, b);
            }
        }
        return try self.readBlock(block_idx);
    }

    // find the block that may contain the key
    pub fn findBlockIndex(self: Self, key: []const u8) !usize {
        // binary search
        var low: usize = 0;
        var high = self.block_metas.len - 1;
        while (low <= high) {
            const mid = low + (high - low) / 2;
            const first_key = self.block_metas[mid].first_key;
            const last_key = self.block_metas[mid].last_key;
            std.debug.print("mid: {d}, first_key: {s}, last_key: {s}\n", .{ mid, first_key, last_key });
            if (std.mem.lessThan(u8, key, first_key)) {
                high = mid - 1;
            } else if (std.mem.lessThan(u8, last_key, key)) {
                low = mid + 1;
            } else return mid;
        }
        return Err.NotFound;
    }

    pub fn numBlocks(self: Self) usize {
        return self.block_metas.len;
    }

    pub fn firstKey(self: Self) []const u8 {
        return self.first_key;
    }

    pub fn lastKey(self: Self) []const u8 {
        return self.last_key;
    }

    pub fn tableSize(self: Self) u64 {
        return self.file.size;
    }

    pub fn sstId(self: Self) u64 {
        return self.id;
    }

    pub fn maxTimestamp(self: Self) u64 {
        return self.max_ts;
    }
};

test "table_meta" {
    const tms = [_]BlockMeta{
        .{ .allocator = std.testing.allocator, .offset = 100, .first_key = "abc", .last_key = "def" },
        .{ .allocator = std.testing.allocator, .offset = 200, .first_key = "ghi", .last_key = "jkl" },
        .{ .allocator = std.testing.allocator, .offset = 300, .first_key = "mno", .last_key = "pqr" },
        .{ .allocator = std.testing.allocator, .offset = 400, .first_key = "stu", .last_key = "vwx" },
        .{ .allocator = std.testing.allocator, .offset = 500, .first_key = "yz", .last_key = "ab" },
    };

    const encoded = try BlockMeta.batchEncode(tms[0..], std.testing.allocator);
    defer std.testing.allocator.free(encoded);

    const metas = try BlockMeta.batchDeecode(encoded, std.testing.allocator);
    defer std.testing.allocator.free(metas);

    for (metas) |tm| {
        std.debug.print("{s} {s}\n", .{ tm.first_key, tm.last_key });
        var tmm = tm;
        tmm.deinit();
    }
}

test "builder" {
    defer {
        std.fs.cwd().deleteTree("./tmp/test.sst") catch {
            std.debug.panic("delete tmp dir failed", .{});
        };
    }
    var sb = try SsTableBuilder.init(std.testing.allocator, 256);
    defer sb.deinit();

    for (0..96) |i| {
        var kb: [64]u8 = undefined;
        var vb: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&kb, "key{:0>5}", .{i});
        const value = try std.fmt.bufPrint(&vb, "value{:0>5}", .{i});
        try sb.add(key, value);
    }

    const cache = try BlockCache.init(std.testing.allocator, 1024);
    var tb = try sb.build(1, cache, "./tmp/test.sst");
    for (tb.block_metas) |tm| {
        std.debug.print("{s} {s}\n", .{ tm.first_key, tm.last_key });
    }
    defer tb.deinit();
}

test "open" {
    defer {
        std.fs.cwd().deleteTree("./tmp/test_open.sst") catch {
            std.debug.panic("delete tmp dir failed", .{});
        };
    }
    var sb = try SsTableBuilder.init(std.testing.allocator, 256);
    defer sb.deinit();

    for (0..96) |i| {
        var kb: [64]u8 = undefined;
        var vb: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&kb, "key{:0>5}", .{i});
        const value = try std.fmt.bufPrint(&vb, "value{:0>5}", .{i});
        try sb.add(key, value);
    }

    var t = try sb.build(
        1,
        try BlockCache.init(std.testing.allocator, 1024),
        "./tmp/test_open.sst",
    );
    t.deinit();

    const f = try FileObject.open("./tmp/test_open.sst");
    var tb = try SsTable.open(
        std.testing.allocator,
        1,
        f,
        try BlockCache.init(std.testing.allocator, 1024),
    );
    defer tb.deinit();

    std.debug.print("num blocks: {d}\n", .{tb.numBlocks()});
    std.debug.print("first key: {s}\n", .{tb.firstKey()});
    std.debug.print("last key: {s}\n", .{tb.lastKey()});
    std.debug.print("table size: {d}\n", .{tb.tableSize()});

    std.debug.print("------------ read blocks: ----------\n", .{});
    const BlockIterator = block.BlockIterator;
    for (0..tb.numBlocks()) |i| {
        std.debug.print("------------- block {d} --------------\n", .{i});
        var b = try tb.readBlock(i);
        defer b.deinit();
        var bi = BlockIterator.createAndSeekToFirst(std.testing.allocator, b);
        defer bi.deinit();
        while (!bi.isEmpty()) {
            std.debug.print("key: {s}, value: {s}\n", .{ bi.key(), bi.value() });
            bi.next();
        }
    }

    std.debug.print("------------ find block: ----------\n", .{});
    try std.testing.expectEqual(try tb.findBlockIndex("key00024"), 2);
    try std.testing.expectEqual(try tb.findBlockIndex("key00030"), 2);
    try std.testing.expectEqual(try tb.findBlockIndex("key00035"), 2);
}
