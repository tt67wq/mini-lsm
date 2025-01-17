const std = @import("std");
const MemTable = @import("memtable.zig");
const iterators = @import("iterators.zig");
const MergeIterators = @import("MergeIterators.zig");
const ss_table = @import("ss_table.zig");
const atomic = std.atomic;
const Bound = MemTable.Bound;
const MemTableIterator = MemTable.MemTableIterator;
const StorageIterator = iterators.StorageIterator;
const LsmIterator = iterators.LsmIterator;
const TwoMergeIterator = iterators.TwoMergeIterator;
const SsTable = ss_table.SsTable;
const SsTableIterator = ss_table.SsTableIterator;
const SsTableBuilder = ss_table.SsTableBuilder;
const BlockCache = ss_table.BlockCache;

pub const StorageOptions = struct {
    block_size: usize,
    target_sst_size: usize,
    num_memtable_limit: usize,
    enable_wal: bool,
};

pub const StorageState = struct {
    allocator: std.mem.Allocator,
    mem_table: atomic.Value(*MemTable),
    imm_mem_tables: std.ArrayList(*MemTable),
    l0_sstables: std.ArrayList(usize),
    sstables: std.AutoHashMap(usize, *SsTable),

    pub fn init(allocator: std.mem.Allocator, _: StorageOptions) StorageState {
        const mm = allocator.create(MemTable) catch unreachable;
        mm.* = MemTable.init(0, allocator, null);
        return StorageState{
            .allocator = allocator,
            .mem_table = atomic.Value(*MemTable).init(mm),
            .imm_mem_tables = std.ArrayList(*MemTable).init(allocator),
            .l0_sstables = std.ArrayList(usize).init(allocator),
            .sstables = std.AutoHashMap(usize, *SsTable).init(allocator),
        };
    }

    pub fn deinit(self: *StorageState) void {
        var mm = self.getMemTable();
        mm.deinit();
        self.allocator.destroy(mm);
        // free imm_mem_tables
        {
            for (self.imm_mem_tables.items) |m| {
                var imm_table = m;
                imm_table.deinit();
                self.allocator.destroy(imm_table);
            }
            self.imm_mem_tables.deinit();
        }

        self.l0_sstables.deinit();

        // free sstables
        {
            var vi = self.sstables.valueIterator();
            while (true) {
                if (vi.next()) |e| {
                    e.*.deinit();
                    self.allocator.destroy(e.*);
                } else {
                    break;
                }
            }
            self.sstables.deinit();
        }
    }

    pub fn getMemTable(self: *StorageState) *MemTable {
        return self.mem_table.load(.seq_cst);
    }
};

pub const WriteBatchRecord = union(enum) {
    put: struct {
        key: []const u8,
        value: []const u8,
    },
    delete: []const u8,
};

pub const StorageInner = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    state: StorageState,
    state_lock: std.Thread.RwLock,
    next_sst_id: atomic.Value(usize),
    path: []const u8,
    options: StorageOptions,
    block_cache: *BlockCache,

    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) !Self {
        var state = StorageState.init(allocator, options);
        const next_sst_id: usize = 0;
        // cache
        const cache = try allocator.create(BlockCache);
        cache.* = try BlockCache.init(allocator, 1 << 20); // 4G
        errdefer cache.deinit();

        // manifest
        const manifest_path = try std.fs.path.join(allocator, &[_][]const u8{
            path,
            "MANIFEST",
        });
        defer allocator.free(manifest_path);

        var manifest_file_exists = true;
        std.fs.cwd().access(manifest_path, .{}) catch |err| switch (err) {
            error.FileNotFound => manifest_file_exists = false,
            else => unreachable,
        };

        if (manifest_file_exists) {
            // TODO: recover manifest
            unreachable;
        } else {
            if (options.enable_wal) {
                const wal_path = try pathOfWal(allocator, path, next_sst_id);
                defer allocator.free(wal_path);
                const new_mm = try allocator.create(MemTable);
                new_mm.* = MemTable.init(next_sst_id, allocator, wal_path);
                var old_mm = state.mem_table.swap(new_mm, .seq_cst);
                old_mm.deinit();
                allocator.destroy(old_mm);
            }
        }
        return Self{
            .allocator = allocator,
            .path = path,
            .state = state,
            .state_lock = .{},
            .next_sst_id = atomic.Value(usize).init(next_sst_id + 1),
            .options = options,
            .block_cache = cache,
        };
    }

    pub fn deinit(self: *Self) void {
        self.state.deinit();
        // free block_cache
        self.block_cache.deinit();
        self.allocator.destroy(self.block_cache);
    }

    fn get_next_sst_id(self: *Self) usize {
        return self.next_sst_id.fetchAdd(1, .seq_cst);
    }

    fn pathOfWal(
        allocator: std.mem.Allocator,
        path: []const u8,
        id: usize,
    ) ![:0]u8 {
        var buf: [9]u8 = undefined;
        const ww = try std.fmt.bufPrint(&buf, "{d:0>5}.wal", .{id});
        return std.fs.path.joinZ(allocator, &[_][]const u8{ path, ww });
    }

    pub fn get(self: *Self, key: []const u8, value: *[]const u8) !bool {
        // search in memtable
        if (try self.state.getMemTable().get(key, value)) {
            if (value.*.len == 0) {
                // tomestone
                return false;
            }
            return true;
        }
        // search in imm_memtables
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.imm_mem_tables.items) |imm_table| {
                if (try imm_table.get(key, value)) {
                    if (value.*.len == 0) {
                        // tomestone
                        return false;
                    }
                    return true;
                }
            }
        }

        // search in l0_sstables
        var iters = std.ArrayList(StorageIterator).init(self.allocator);
        defer {
            for (iters.items) |iter| {
                var ii = iter;
                ii.deinit();
            }
            iters.deinit();
        }
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.l0_sstables.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                if (try sst.*.mayContain(key)) {
                    var ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, sst, key);
                    errdefer ss_iter.deinit();
                    try iters.append(.{ .ss_table_iter = ss_iter });
                }
            }
        }
        var l0_iters = try MergeIterators.init(self.allocator, iters);
        defer l0_iters.deinit();

        if (std.mem.eql(u8, l0_iters.key(), key) and l0_iters.value().len > 0) {
            value.* = l0_iters.value();
            return true;
        }

        return false;
    }

    pub fn writeBatch(self: *Self, records: []const WriteBatchRecord) !void {
        for (records) |record| {
            switch (record) {
                .put => |pp| {
                    try self.state.getMemTable().put(pp.key, pp.value);
                    try self.tryFreeze(self.state.getMemTable().getApproximateSize());
                },
                .delete => |dd| {
                    // we use "" as the tombstone value
                    try self.state.getMemTable().put(dd, "");
                    try self.tryFreeze(self.state.getMemTable().getApproximateSize());
                },
            }
        }
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        return self.writeBatch(&[_]WriteBatchRecord{
            .{
                .put = .{
                    .key = key,
                    .value = value,
                },
            },
        });
    }

    pub fn delete(self: *Self, key: []const u8) !void {
        return self.writeBatch(&[_]WriteBatchRecord{
            .{
                .delete = key,
            },
        });
    }

    fn tryFreeze(self: *Self, estimate_size: usize) !void {
        if (estimate_size < self.options.target_sst_size) {
            return;
        }

        self.state_lock.lockShared();
        errdefer self.state_lock.unlockShared();
        // double check
        if (self.state.getMemTable().getApproximateSize() >= self.options.target_sst_size) {
            self.state_lock.unlockShared();
            try self.forceFreezeMemtable();
            return;
        }
        self.state_lock.unlockShared();
    }

    fn forceFreezeMemtable(self: *Self) !void {
        const next_sst_id = self.get_next_sst_id();
        const new_mm = try self.allocator.create(MemTable);
        errdefer self.allocator.destroy(new_mm);
        {
            if (self.options.enable_wal) {
                const mm_path = try pathOfWal(self.allocator, self.path, next_sst_id);
                defer self.allocator.free(mm_path);
                new_mm.* = MemTable.init(next_sst_id, self.allocator, mm_path);
            } else {
                new_mm.* = MemTable.init(next_sst_id, self.allocator, null);
            }
        }
        errdefer new_mm.deinit();
        var old_mm: *MemTable = undefined;
        {
            self.state_lock.lock();
            defer self.state_lock.unlock();
            old_mm = self.state.mem_table.swap(new_mm, .seq_cst);
            try self.state.imm_mem_tables.insert(0, old_mm);
        }
        try old_mm.syncWal();
    }

    fn rangeOverlap(
        user_begin: Bound,
        user_end: Bound,
        sst_begin: []const u8,
        sst_end: []const u8,
    ) bool {
        switch (user_end.bound_t) {
            .excluded => {
                // user_end.key <= sst_begin
                // !(user_end.key > sst_begin)
                if (!std.mem.order(u8, user_end.data, sst_begin) == .gt) {
                    return false;
                }
            },
            .included => {
                // user_begin.key < sst_begin
                if (std.mem.lessThan(u8, user_end.data, sst_begin)) {
                    return false;
                }
            },
            inline else => {},
        }

        switch (user_begin.bound_t) {
            .excluded => {
                // user_begin.key >= sst_end
                // !(sst_end.key < user_begin)
                if (!std.mem.lessThan(u8, sst_end, user_begin.data)) {
                    return false;
                }
            },
            .included => {
                // user_begin.key > sst_end
                if (std.mem.order(u8, user_begin.data, sst_end) == .gt) {
                    return false;
                }
            },
            inline else => {},
        }
        return true;
    }

    pub fn scan(self: *Self, lower: Bound, upper: Bound) !LsmIterator {
        var memtable_iters = std.ArrayList(StorageIterator).init(self.allocator);
        defer memtable_iters.deinit();

        // collect memtable iterators
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.imm_mem_tables.items) |imm_table| {
                try memtable_iters.append(
                    .{ .mem_iter = imm_table.scan(lower, upper) },
                );
            }
            try memtable_iters.append(
                .{ .mem_iter = self.state.getMemTable().scan(lower, upper) },
            );
        }

        const mi = try MergeIterators.init(self.allocator, memtable_iters.items);
        errdefer mi.deinit();

        // l0_sst
        var sst_iters = std.ArrayList(StorageIterator).init(self.allocator);
        defer {
            for (sst_iters.items) |iter| {
                iter.deinit();
            }
            sst_iters.deinit();
        }

        // collect sst iterators
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.l0_sstables.items) |sst_id| {
                const table = self.state.sstables.get(sst_id).?;
                if (rangeOverlap(lower, upper, table.firstKey(), table.lastKey())) {
                    var ss_iter: SsTableIterator = undefined;
                    switch (lower.bound_t) {
                        .included => {
                            ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, table, lower.data);
                            errdefer ss_iter.deinit();
                        },
                        .excluded => {
                            ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, table, lower.data);
                            errdefer ss_iter.deinit();
                            if (!ss_iter.isEmpty() and std.mem.eql(u8, ss_iter.key(), lower.data)) {
                                ss_iter.next();
                            }
                        },
                        .unbounded => {
                            ss_iter = try SsTableIterator.initAndSeekToFirst(self.allocator, table);
                            errdefer ss_iter.deinit();
                        },
                    }
                    try sst_iters.append(.{ .ss_table_iter = ss_iter });
                }
            }
        }

        return LsmIterator.init(TwoMergeIterator.init(mi, sst_iters.items), upper);
    }

    fn pathOfSst(self: Self, sst_id: usize) ![:0]u8 {
        var buf: [9]u8 = undefined;
        const ww = try std.fmt.bufPrint(&buf, "{d:0>5}.sst", .{sst_id});
        return std.fs.path.join(self.allocator, &[_][]const u8{ self.path, ww });
    }

    pub fn flushNextMemtable(self: *Self) !void {
        var to_flush_table: *MemTable = undefined;
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            to_flush_table = self.state.imm_mem_tables.getLast();
        }

        var builder = try SsTableBuilder.init(self.allocator, self.options.block_size);
        defer builder.deinit();

        const sst_id = to_flush_table.id;
        try to_flush_table.flush(&builder);

        const sst = try self.state.allocator.create(SsTable);
        errdefer self.state.allocator.destroy(sst);
        sst.* = try builder.build(sst_id, self.block_cache, try self.pathOfSst(sst_id));
        errdefer sst.deinit();

        // add the flushed table to l0_sstables
        {
            self.state_lock.lock();
            defer self.state_lock.unlock();

            const m = self.state.imm_mem_tables.pop();
            defer m.deinit();
            std.debug.assert(m.id == sst_id);
            try self.state.l0_sstables.insert(0, sst_id);
            try self.state.sstables.put(sst.id, sst);
        }
    }
};

test "init" {
    defer std.fs.cwd().deleteTree("./tmp/storage/init") catch unreachable;
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };

    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/init", opts);
    defer storage.deinit();
}

test "put/delete/get" {
    defer std.fs.cwd().deleteTree("./tmp/storage/put") catch unreachable;
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/put", opts);
    defer storage.deinit();

    try storage.put("key1", "value1");
    try storage.put("key2", "value2");
    try storage.put("key3", "value3");
    try storage.delete("key3");

    var value: []const u8 = undefined;
    if (try storage.get("key1", &value)) {
        std.debug.print("key1: {s}\n", .{value});
        try std.testing.expectEqualStrings("value1", value);
    }

    if (try storage.get("key2", &value)) {
        std.debug.print("key2: {s}\n", .{value});
        try std.testing.expectEqualStrings("value2", value);
    }

    if (try storage.get("key3", &value)) {
        unreachable;
    }
}

test "freeze" {
    defer std.fs.cwd().deleteTree("./tmp/storage/freeze") catch unreachable;
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/freeze", opts);
    defer storage.deinit();

    for (0..160) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = std.fmt.bufPrint(&kb, "key{d:0>5}", .{i}) catch unreachable;
        const vv = std.fmt.bufPrint(&vb, "val{d:0>5}", .{i}) catch unreachable;
        std.debug.print("put {s} {s}\n", .{ kk, vv });
        try storage.put(kk, vv);
    }

    try std.testing.expectEqual(storage.state.imm_mem_tables.items.len, 2);
}

test "flush" {}
