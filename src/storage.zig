const std = @import("std");
const MemTable = @import("MemTable.zig");
const iterators = @import("iterators.zig");
const MergeIterators = @import("MergeIterators.zig");
const ss_table = @import("ss_table.zig");
const compact = @import("compact.zig");
const atomic = std.atomic;
const Bound = MemTable.Bound;
const MemTablePtr = MemTable.MemTablePtr;
const MemTableIterator = MemTable.MemTableIterator;
const StorageIterator = iterators.StorageIterator;
const StorageIteratorPtr = iterators.StorageIteratorPtr;
const CombinedIteratorPtr = iterators.CombinedIteratorPtr;
const SstConcatIterator = iterators.SstConcatIterator;
const LsmIterator = iterators.LsmIterator;
const TwoMergeIterator = iterators.TwoMergeIterator;
const SsTable = ss_table.SsTable;
const SsTablePtr = ss_table.SsTablePtr;
const SsTableIterator = ss_table.SsTableIterator;
const SsTableBuilder = ss_table.SsTableBuilder;
const BlockCache = ss_table.BlockCache;
const BlockCachePtr = ss_table.BlockCachePtr;
const CompactionTask = compact.CompactionTask;
const ForceFullCompaction = compact.ForceFullCompaction;

pub const StorageOptions = struct {
    block_size: usize,
    target_sst_size: usize,
    num_memtable_limit: usize,
    enable_wal: bool,
    compaction_options: compact.CompactionOptions,
};

pub const StorageState = struct {
    allocator: std.mem.Allocator,
    mem_table: MemTablePtr,
    imm_mem_tables: std.ArrayList(MemTablePtr),
    l0_sstables: std.ArrayList(usize),
    levels: std.ArrayList(std.ArrayList(usize)),
    sstables: std.AutoHashMap(usize, SsTablePtr),

    pub fn init(allocator: std.mem.Allocator, options: StorageOptions) !StorageState {
        var mm = MemTable.init(0, allocator, null);
        errdefer mm.deinit();

        var levels = std.ArrayList(std.ArrayList(usize)).init(allocator);
        switch (options.compaction_options) {
            .no_compaction => {
                const lv1 = std.ArrayList(usize).init(allocator);
                try levels.append(lv1);
            },
        }
        return StorageState{
            .allocator = allocator,
            .mem_table = try MemTablePtr.create(allocator, mm),
            .imm_mem_tables = std.ArrayList(MemTablePtr).init(allocator),
            .l0_sstables = std.ArrayList(usize).init(allocator),
            .levels = levels,
            .sstables = std.AutoHashMap(usize, SsTablePtr).init(allocator),
        };
    }

    pub fn deinit(self: *StorageState) void {
        // free mem_table
        self.mem_table.release();

        // free imm_mem_tables
        {
            for (self.imm_mem_tables.items) |m| {
                var imm_table = m;
                imm_table.deinit();
            }
            self.imm_mem_tables.deinit();
        }

        self.l0_sstables.deinit();

        // free sstables
        {
            var vi = self.sstables.valueIterator();
            while (true) {
                if (vi.next()) |e| {
                    e.release();
                } else {
                    break;
                }
            }
            self.sstables.deinit();
        }
        // free levels
        {
            for (self.levels.items) |l| {
                l.deinit();
            }
            self.levels.deinit();
        }
    }

    pub fn getMemTable(self: StorageState) *MemTable {
        return self.mem_table.get();
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
    state_lock: std.Thread.RwLock = .{},
    next_sst_id: atomic.Value(usize),
    path: []const u8,
    options: StorageOptions,
    block_cache: BlockCachePtr,
    terminate: std.Thread.ResetEvent = .{},
    wg: std.Thread.WaitGroup = .{},

    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) !Self {
        var state = try StorageState.init(allocator, options);
        const next_sst_id: usize = 0;
        // cache
        var cache = try BlockCache.init(allocator, 1 << 20); // 4G
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
            else => return err,
        };

        if (manifest_file_exists) {
            // TODO: recover manifest
            unreachable;
        } else {
            if (options.enable_wal) {
                const wal_path = try pathOfWal(allocator, path, next_sst_id);
                defer allocator.free(wal_path);
                var new_mm = MemTable.init(next_sst_id, allocator, wal_path);
                errdefer new_mm.deinit();
                state.mem_table.release();
                state.mem_table = try MemTablePtr.create(allocator, new_mm);
            }
        }
        return Self{
            .allocator = allocator,
            .path = path,
            .state = state,
            .next_sst_id = atomic.Value(usize).init(next_sst_id + 1),
            .options = options,
            .block_cache = try BlockCachePtr.create(allocator, cache),
        };
    }

    pub fn deinit(self: *Self) void {
        // stop compaction thread
        self.terminate.set();
        self.wg.wait();

        // free state
        self.state.deinit();

        // free block_cache
        self.block_cache.deinit();
    }

    pub fn syncDir(self: Self) !void {
        const DirSyncer = struct {
            fn sync(dir: std.fs.Dir) !void {
                var it = dir.iterate();
                while (try it.next()) |en| {
                    switch (en.kind) {
                        .file => {
                            try syncFile(dir, en.name);
                        },
                        .directory => {
                            var sd = try dir.openDir(en.name, .{});
                            defer sd.close();
                            try sync(sd);
                        },
                        inline else => {},
                    }
                }
            }

            fn syncFile(dir: std.fs.Dir, name: []const u8) !void {
                var file = try dir.openFile(name, .{});
                defer file.close();
                try file.sync();
            }
        };
        var dir = try std.fs.cwd().openDir(self.path, .{});
        defer dir.close();

        try DirSyncer.sync(dir);
    }

    fn getNextSstId(self: *Self) usize {
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
                if (try imm_table.load().get(key, value)) {
                    if (value.*.len == 0) {
                        // tomestone
                        return false;
                    }
                    return true;
                }
            }
        }

        // search in l0_sstables
        var iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer {
            for (iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            iters.deinit();
        }
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.l0_sstables.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                if (try sst.load().mayContain(key)) {
                    var ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, sst.clone(), key);
                    errdefer ss_iter.deinit();
                    try iters.append(try StorageIteratorPtr.create(self.allocator, .{ .ss_table_iter = ss_iter }));
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
        const next_sst_id = self.getNextSstId();
        var new_mm: MemTable = undefined;
        {
            if (self.options.enable_wal) {
                const mm_path = try pathOfWal(self.allocator, self.path, next_sst_id);
                defer self.allocator.free(mm_path);
                new_mm = MemTable.init(next_sst_id, self.allocator, mm_path);
            } else {
                new_mm = MemTable.init(next_sst_id, self.allocator, null);
            }
        }
        errdefer new_mm.deinit();

        var old_mm: *MemTable = undefined;
        {
            self.state_lock.lock();
            defer self.state_lock.unlock();
            var old_mm_ptr = self.state.mem_table;
            old_mm = old_mm_ptr.get();
            defer old_mm_ptr.release();
            self.state.mem_table = try MemTablePtr.create(self.allocator, new_mm);
            try self.state.imm_mem_tables.append(old_mm_ptr.clone()); // newer memtable is inserted at the end
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
                if (!(std.mem.order(u8, user_end.data, sst_begin) == .gt)) {
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
        // memtable iters are sorted, newest memtable at the end
        var memtable_iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer memtable_iters.deinit();

        // collect memtable iterators
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.imm_mem_tables.items) |imm_table| {
                if (!imm_table.load().isEmpty()) {
                    var sp = try StorageIteratorPtr.create(self.allocator, .{
                        .mem_iter = imm_table.load().scan(lower, upper),
                    });
                    errdefer sp.release();
                    try memtable_iters.append(sp);
                }
            }
            var sp = try StorageIteratorPtr.create(self.allocator, .{
                .mem_iter = self.state.getMemTable().scan(lower, upper),
            });
            errdefer sp.release();
            try memtable_iters.append(sp);
        }

        var memtable_merge_iters = try MergeIterators.init(self.allocator, memtable_iters);
        errdefer memtable_merge_iters.deinit();

        // l0_sst
        var l0_iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer l0_iters.deinit();

        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.l0_sstables.items) |sst_id| {
                const table_ptr = self.state.sstables.get(sst_id).?;
                const table = table_ptr.load();
                if (rangeOverlap(lower, upper, table.firstKey(), table.lastKey())) {
                    var ss_iter: SsTableIterator = undefined;
                    switch (lower.bound_t) {
                        .included => {
                            ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, table_ptr, lower.data);
                        },
                        .excluded => {
                            ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, table_ptr, lower.data);
                            if (!ss_iter.isEmpty() and std.mem.eql(u8, ss_iter.key(), lower.data)) {
                                try ss_iter.next();
                            }
                        },
                        .unbounded => {
                            ss_iter = try SsTableIterator.initAndSeekToFirst(self.allocator, table_ptr);
                        },
                    }
                    errdefer ss_iter.deinit();
                    var sp = try StorageIteratorPtr.create(self.allocator, .{
                        .ss_table_iter = ss_iter,
                    });
                    errdefer sp.release();
                    try l0_iters.append(sp);
                }
            }
        }

        var l0_merge_iter = try MergeIterators.init(self.allocator, l0_iters);
        errdefer l0_merge_iter.deinit();

        // levels
        var lv_iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer lv_iters.deinit();
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.levels.items) |l| {
                var lv_ssts = try std.ArrayList(SsTablePtr).initCapacity(self.allocator, l.items.len);
                defer lv_ssts.deinit();
                for (l.items) |sst_id| {
                    const table_ptr = self.state.sstables.get(sst_id).?;
                    const table = table_ptr.load();
                    if (rangeOverlap(lower, upper, table.firstKey(), table.lastKey())) {
                        try lv_ssts.append(table_ptr.clone());
                    }
                }

                var sst_concat_iter: SstConcatIterator = undefined;
                switch (lower.bound_t) {
                    .included => {
                        sst_concat_iter = try SstConcatIterator.initAndSeekToKey(self.allocator, lv_ssts, lower.data);
                    },
                    .excluded => {
                        sst_concat_iter = try SstConcatIterator.initAndSeekToKey(self.allocator, lv_ssts, lower.data);
                        if (!sst_concat_iter.isEmpty() and std.mem.eql(u8, sst_concat_iter.key(), lower.data)) {
                            try sst_concat_iter.next();
                        }
                    },
                    .unbounded => {
                        sst_concat_iter = try SstConcatIterator.initAndSeekToFirst(self.allocator, lv_ssts);
                    },
                }
                errdefer sst_concat_iter.deinit();
                var sp = try StorageIteratorPtr.create(self.allocator, .{
                    .sst_concat_iter = sst_concat_iter,
                });
                errdefer sp.release();
                try lv_iters.append(sp);
            }
        }

        var lv_merge_iters = try MergeIterators.init(self.allocator, lv_iters);
        errdefer lv_merge_iters.deinit();

        var iter = try TwoMergeIterator.init(
            try CombinedIteratorPtr.create(self.allocator, .{ .merge_iterators = memtable_merge_iters }),
            try CombinedIteratorPtr.create(self.allocator, .{ .merge_iterators = l0_merge_iter }),
        );
        errdefer iter.deinit();

        iter = try TwoMergeIterator.init(
            try CombinedIteratorPtr.create(self.allocator, .{ .two_merge_iter = iter }),
            try CombinedIteratorPtr.create(self.allocator, .{ .merge_iterators = lv_merge_iters }),
        );

        return LsmIterator.init(iter, upper);
    }

    fn pathOfSst(self: Self, sst_id: usize) ![]u8 {
        var buf: [9]u8 = undefined;
        const ww = try std.fmt.bufPrint(&buf, "{d:0>5}.sst", .{sst_id});
        return std.fs.path.join(self.allocator, &[_][]const u8{ self.path, ww });
    }

    pub fn flushNextMemtable(self: *Self) !void {
        std.debug.assert(self.state.imm_mem_tables.items.len > 0);
        var to_flush_table: *MemTable = undefined;
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            // oldest memtable is at the index 0
            to_flush_table = self.state.imm_mem_tables.items[0].load();
        }

        var builder = try SsTableBuilder.init(self.allocator, self.options.block_size);
        defer builder.deinit();

        const sst_id = to_flush_table.id;
        try to_flush_table.flush(&builder);

        const sst_path = try self.pathOfSst(sst_id);
        defer self.allocator.free(sst_path);
        var sst = try builder.build(sst_id, self.block_cache.clone(), sst_path);
        errdefer sst.deinit();

        // add the flushed table to l0_sstables
        {
            self.state_lock.lock();
            defer self.state_lock.unlock();

            var m = self.state.imm_mem_tables.orderedRemove(0);
            defer m.deinit();
            std.debug.assert(m.load().id == sst_id);

            // newest sstable is at the end
            try self.state.l0_sstables.append(sst_id);
            try self.state.sstables.put(sst.id, try SsTablePtr.create(self.allocator, sst));
        }
    }

    fn triggerFlush(self: *Self) !void {
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            if (self.state.imm_mem_tables.items.len < self.options.num_memtable_limit) {
                return;
            }
        }
        try self.flushNextMemtable();
    }

    fn flushLoop(self: *Self) !void {
        while (!self.terminate.isSet()) {
            try self.triggerFlush();
            try self.terminate.timedWait(50 * std.time.ns_per_ms);
        }
    }

    pub fn spawnCompactionThread(self: *Self) void {
        self.wg.spawnManager(self.flushLoop, .{});
    }

    pub fn forceFullCompaction(self: *Self) !void {
        if (!self.options.compaction_options.is_no_compaction()) {
            @panic("full compaction can only be called with compaction is not enabled");
        }

        var l0_sstables: std.ArrayList(usize) = undefined;
        defer l0_sstables.deinit();
        var l1_sstables: std.ArrayList(usize) = undefined;
        defer l1_sstables.deinit();
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            l0_sstables = try self.state.l0_sstables.clone();
            errdefer l0_sstables.deinit();
            l1_sstables = try self.state.levels.getLast().clone();
            errdefer l1_sstables.deinit();
        }

        const compation_task = CompactionTask{
            .force_full_compaction = .{
                .l0_sstables = l0_sstables,
                .l1_sstables = l1_sstables,
            },
        };

        var sstables = try self.compact(compation_task);
        defer {
            for (sstables.items) |sst| {
                var p = sst;
                p.release();
            }
            sstables.deinit();
        }
        var ids = try std.ArrayList(usize).initCapacity(self.allocator, sstables.items.len);
        defer ids.deinit();

        {
            self.state_lock.lock();
            defer self.state_lock.unlock();

            for (l0_sstables.items) |id| {
                std.debug.assert(self.state.sstables.remove(id));
            }
            for (l1_sstables.items) |id| {
                std.debug.assert(self.state.sstables.remove(id));
            }
            for (sstables.items) |sst| {
                const id: usize = @intCast(sst.get().sstId());
                try ids.append(id);
                try self.state.sstables.put(id, sst);
            }
            var old_l1 = self.state.levels.pop();
            old_l1.deinit();
            try self.state.levels.append(try ids.clone());

            // state.l0_sstables may change after compaction
            {
                var l0_sstables_map = std.AutoHashMap(usize, struct {}).init(self.allocator);
                defer l0_sstables_map.deinit();

                for (l0_sstables.items) |id| {
                    try l0_sstables_map.put(id, .{});
                }

                var new_l0_sstables = std.ArrayList(usize).init(self.allocator);
                errdefer new_l0_sstables.deinit();
                for (self.state.l0_sstables.items) |id| {
                    if (!l0_sstables_map.contains(id)) {
                        try new_l0_sstables.append(id);
                    }
                }
                self.state.l0_sstables.deinit();
                self.state.l0_sstables = new_l0_sstables;
            }
            try self.syncDir();
        }
        // remove old sst files
        {
            for (l0_sstables.items) |id| {
                const path = try self.pathOfSst(id);
                defer self.allocator.free(path);
                try std.fs.cwd().deleteFile(path);
            }
            for (l1_sstables.items) |id| {
                const path = try self.pathOfSst(id);
                defer self.allocator.free(path);
                try std.fs.cwd().deleteFile(path);
            }
        }
    }

    fn compact(self: *Self, task: CompactionTask) !std.ArrayList(SsTablePtr) {
        switch (task) {
            .force_full_compaction => |f| {
                return self.compactForceFull(f);
            },
        }
    }

    fn compactForceFull(self: *Self, f: ForceFullCompaction) !std.ArrayList(SsTablePtr) {
        const l0_sstables = f.l0_sstables;
        const l1_sstables = f.l1_sstables;

        var l0_iters = try std.ArrayList(StorageIteratorPtr).initCapacity(self.allocator, l0_sstables.items.len);
        defer l0_iters.deinit();

        for (l0_sstables.items) |sst_id| {
            self.state_lock.lockShared();
            const sst = self.state.sstables.get(sst_id).?;
            self.state_lock.unlockShared();
            var ss_iter = try SsTableIterator.initAndSeekToFirst(self.allocator, sst);
            errdefer ss_iter.deinit();
            const sp = try StorageIteratorPtr.create(self.allocator, .{
                .ss_table_iter = ss_iter,
            });
            try l0_iters.append(sp);
        }

        var l1_tables = try std.ArrayList(SsTablePtr).initCapacity(self.allocator, l1_sstables.items.len);
        errdefer l1_tables.deinit();
        for (l1_sstables.items) |sst_id| {
            self.state_lock.lockShared();
            const sst = self.state.sstables.get(sst_id).?;
            self.state_lock.unlockShared();
            try l1_tables.append(sst.clone());
        }

        var l0_merge_iter = try MergeIterators.init(self.allocator, l0_iters);
        errdefer l0_merge_iter.deinit();
        var l1_sst_iter = try SstConcatIterator.initAndSeekToFirst(self.allocator, l1_tables);
        errdefer l1_sst_iter.deinit();

        var iter = try TwoMergeIterator.init(
            try CombinedIteratorPtr.create(self.allocator, .{ .merge_iterators = l0_merge_iter }),
            try CombinedIteratorPtr.create(self.allocator, .{ .sst_concat_iter = l1_sst_iter }),
        );
        defer iter.deinit();

        return self.compactGenerateSstFromIter(&iter, true);
    }

    fn compactGenerateSstFromIter(self: *Self, iter: *TwoMergeIterator, compact_to_bottom_level: bool) !std.ArrayList(SsTablePtr) {
        var builder: SsTableBuilder = undefined;
        var initialized = false;
        defer {
            if (initialized) {
                builder.deinit();
            }
        }
        var new_ssts = std.ArrayList(SsTablePtr).init(self.allocator);
        while (!iter.isEmpty()) {
            if (!initialized) {
                builder = try SsTableBuilder.init(self.allocator, self.options.block_size);
                initialized = true;
            }
            if (compact_to_bottom_level) {
                if (iter.value().len > 0) {
                    try builder.add(iter.key(), iter.value());
                }
            } else {
                try builder.add(iter.key(), iter.value());
            }
            try iter.next();

            if (builder.estimated_size() >= self.options.target_sst_size) {
                const sst_id = self.getNextSstId();
                const path = try self.pathOfSst(sst_id);
                defer self.allocator.free(path);
                var sst = try builder.build(sst_id, self.block_cache.clone(), path);
                errdefer sst.deinit();

                var sst_ptr = try SsTablePtr.create(self.allocator, sst);
                errdefer sst_ptr.deinit();

                try new_ssts.append(sst_ptr);
            }
        }
        // iter is empty
        if (initialized) {
            const sst_id = self.getNextSstId();
            const path = try self.pathOfSst(sst_id);
            defer self.allocator.free(path);
            var sst = try builder.build(sst_id, self.block_cache.clone(), path);
            errdefer sst.deinit();
            var sst_ptr = try SsTablePtr.create(self.allocator, sst);
            errdefer sst_ptr.deinit();
            try new_ssts.append(sst_ptr);
        }
        return new_ssts;
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

test "flush" {
    defer std.fs.cwd().deleteTree("./tmp/storage/flush") catch unreachable;
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };

    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/flush", opts);
    defer storage.deinit();

    for (0..256) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);
    }

    const l1 = storage.state.imm_mem_tables.items.len;

    try storage.flushNextMemtable();
    const l2 = storage.state.imm_mem_tables.items.len;

    try std.testing.expectEqual(l1, l2 + 1);
    try std.testing.expectEqual(storage.state.l0_sstables.items.len, 1);
}

test "scan" {
    defer std.fs.cwd().deleteTree("./tmp/storage/scan") catch unreachable;
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{ .no_compaction = .{} },
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/scan", opts);
    defer storage.deinit();

    for (0..256) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);
    }

    try storage.flushNextMemtable();

    var iter = try storage.scan(
        Bound.init("", .unbounded),
        Bound.init("", .unbounded),
    );
    defer iter.deinit();

    while (!iter.isEmpty()) {
        std.debug.print("key: {s} value: {s}\n", .{ iter.key(), iter.value() });
        try iter.next();
    }
}

test "compact" {
    defer std.fs.cwd().deleteTree("./tmp/storage/compact") catch unreachable;
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{ .no_compaction = .{} },
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/compact", opts);
    defer storage.deinit();

    for (0..1024) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);
    }

    try storage.triggerFlush();
    std.debug.print("l0 size: {d}\n", .{storage.state.l0_sstables.items.len});
    try storage.forceFullCompaction();
    std.debug.print("l0 size: {d}\n", .{storage.state.l0_sstables.items.len});
}
