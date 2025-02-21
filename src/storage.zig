const std = @import("std");
const MemTable = @import("MemTable.zig");
const iterators = @import("iterators.zig");
const MergeIterators = @import("MergeIterators.zig");
const ss_table = @import("ss_table.zig");
const compact = @import("compact.zig");
const smart_pointer = @import("smart_pointer.zig");
const atomic = std.atomic;
const Bound = MemTable.Bound;
const MemTablePtr = MemTable.MemTablePtr;
const MemTableIterator = MemTable.MemTableIterator;
const StorageIterator = iterators.StorageIterator;
const StorageIteratorPtr = iterators.StorageIteratorPtr;
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
const CompactionController = compact.CompactionController;
const SimpleLeveledCompactionController = compact.SimpleLeveledCompactionController;
const SimpleLeveledCompactionTask = compact.SimpleLeveledCompactionTask;
const TieredCompactionController = compact.TieredCompactionController;
const TieredCompactionTask = compact.TieredCompactionTask;

pub const StorageOptions = struct {
    block_size: usize,
    target_sst_size: usize,
    num_memtable_limit: usize,
    enable_wal: bool = true,
    compaction_options: compact.CompactionOptions = .{ .no_compaction = .{} },
};

pub const Level = struct {
    tiered_id: usize = 0,
    ssts: std.ArrayList(usize),

    pub fn init(tiered_id: usize, allocator: std.mem.Allocator) Level {
        return .{
            .tiered_id = tiered_id,
            .ssts = std.ArrayList(usize).init(allocator),
        };
    }

    pub fn deinit(self: Level) void {
        self.ssts.deinit();
    }

    pub fn clear(self: *Level) void {
        self.ssts.clearRetainingCapacity();
    }

    pub fn dump(self: Level) !std.ArrayList(usize) {
        return self.ssts.clone();
    }

    pub fn append(self: *Level, sst_id: usize) !void {
        return self.ssts.append(sst_id);
    }

    pub fn size(self: Level) usize {
        return self.ssts.items.len;
    }
};

pub const LevelPtr = smart_pointer.SmartPointer(Level);

pub const StorageState = struct {
    allocator: std.mem.Allocator,
    mem_table: MemTablePtr,
    imm_mem_tables: std.ArrayList(MemTablePtr),
    l0_sstables: LevelPtr,
    levels: std.ArrayList(LevelPtr),
    sstables: std.AutoHashMap(usize, SsTablePtr),

    pub fn init(allocator: std.mem.Allocator, options: StorageOptions) !StorageState {
        var mm = MemTable.init(0, allocator, null);
        errdefer mm.deinit();

        var levels = std.ArrayList(LevelPtr).init(allocator);
        switch (options.compaction_options) {
            .no_compaction => {
                const lv = Level.init(0, allocator);
                try levels.append(try LevelPtr.create(allocator, lv));
            },
            .simple => |option| {
                for (0..option.max_levels) |_| {
                    const lv = Level.init(0, allocator);
                    try levels.append(try LevelPtr.create(allocator, lv));
                }
            },
            inline else => {},
        }
        return StorageState{
            .allocator = allocator,
            .mem_table = try MemTablePtr.create(allocator, mm),
            .imm_mem_tables = std.ArrayList(MemTablePtr).init(allocator),
            .l0_sstables = try LevelPtr.create(allocator, Level.init(0, allocator)),
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
                var lp = l;
                lp.deinit();
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

pub const StorageInnerPtr = smart_pointer.SmartPointer(StorageInner);

pub const StorageInner = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    state: StorageState,
    state_lock: std.Thread.RwLock = .{},
    next_sst_id: atomic.Value(usize),
    path: []const u8,
    options: StorageOptions,
    compaction_controller: CompactionController,
    block_cache: BlockCachePtr,
    terminate: std.Thread.ResetEvent = .{},
    wg: std.Thread.WaitGroup = .{},

    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) !Self {
        var state = try StorageState.init(allocator, options);
        const next_sst_id: usize = 0;
        // cache
        var cache = try BlockCache.init(allocator, 1 << 20); // 4G
        errdefer cache.deinit();

        // compaction controller
        const compaction_controller = switch (options.compaction_options) {
            .simple => |option| CompactionController{ .simple = SimpleLeveledCompactionController.init(option) },
            .tiered => |option| CompactionController{ .tiered = TieredCompactionController.init(option) },
            .no_compaction => CompactionController{ .no_compaction = .{} },
        };

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
            .compaction_controller = compaction_controller,
            .block_cache = try BlockCachePtr.create(allocator, cache),
        };
    }

    pub fn deinit(self: *Self) void {
        self.close() catch |err| {
            std.log.err("close storage error: {any}\n", .{err});
        };
    }

    pub fn close(self: *Self) !void {
        // stop compaction thread
        self.terminate.set();
        self.wg.wait();

        // sync wal
        try self.sync();

        // TODO: flush memtable

        // sync
        try self.sync();
        try self.syncDir();

        // free state
        self.state.deinit();

        // free block_cache
        self.block_cache.deinit();
    }

    pub fn sync(self: *Self) !void {
        if (self.options.enable_wal) {
            try self.state.getMemTable().syncWal();
        }
    }

    pub fn syncDir(self: Self) !void {
        const DirSyncer = struct {
            fn __sync(dir: std.fs.Dir) !void {
                var it = dir.iterate();
                while (try it.next()) |en| {
                    switch (en.kind) {
                        .file => {
                            try syncFile(dir, en.name);
                        },
                        .directory => {
                            var sd = try dir.openDir(en.name, .{});
                            defer sd.close();
                            try __sync(sd);
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

        try DirSyncer.__sync(dir);
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

            // newest located at the end
            for (0..self.state.imm_mem_tables.items.len) |i| {
                const imm_table = self.state.imm_mem_tables.items[self.state.imm_mem_tables.items.len - 1 - i];
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
        var l0_iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer {
            for (l0_iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            l0_iters.deinit();
        }
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.l0_sstables.get().ssts.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                if (try sst.load().mayContain(key)) {
                    var ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, sst.clone(), key);
                    errdefer ss_iter.deinit();
                    if (ss_iter.isEmpty()) {
                        continue;
                    }
                    try l0_iters.append(try StorageIteratorPtr.create(self.allocator, .{ .ss_table_iter = ss_iter }));
                }
            }
        }

        // search in levels
        var level_iters: std.ArrayList(StorageIteratorPtr) = undefined;
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            level_iters = try std.ArrayList(StorageIteratorPtr).initCapacity(
                self.allocator,
                self.state.levels.items.len,
            );
            for (self.state.levels.items) |level| {
                var level_ssts = try std.ArrayList(SsTablePtr).initCapacity(
                    self.allocator,
                    level.get().size(),
                );
                errdefer level_ssts.deinit();
                for (level.get().ssts.items) |sst_id| {
                    const sst = self.state.sstables.get(sst_id).?;
                    if (try mayWithinTable(key, sst)) {
                        try level_ssts.append(sst.clone());
                    }
                }
                if (level_ssts.items.len > 0) {
                    var level_iter = try SstConcatIterator.initAndSeekToKey(
                        self.allocator,
                        level_ssts,
                        key,
                    );
                    errdefer level_iter.deinit();
                    if (level_iter.isEmpty()) {
                        continue;
                    }
                    try level_iters.append(try StorageIteratorPtr.create(self.allocator, .{ .sst_concat_iter = level_iter }));
                }
            }
        }
        defer {
            for (level_iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            level_iters.deinit();
        }
        var l0_merge_iter = try MergeIterators.init(self.allocator, l0_iters);
        errdefer l0_merge_iter.deinit();

        var levels_merge_iter = try MergeIterators.init(self.allocator, level_iters);
        errdefer levels_merge_iter.deinit();

        var iter = try TwoMergeIterator.init(
            try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = l0_merge_iter }),
            try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = levels_merge_iter }),
        );
        defer iter.deinit();

        if (iter.isEmpty()) {
            return false;
        }

        if (std.mem.eql(u8, iter.key(), key) and iter.value().len > 0) {
            value.* = iter.value();
            return true;
        }

        return false;
    }

    fn mayWithinTable(key: []const u8, table: SsTablePtr) !bool {
        const fk = table.load().firstKey();
        const lk = table.load().lastKey();

        if (std.mem.lessThan(u8, key, fk)) {
            return false;
        }
        if (std.mem.lessThan(u8, lk, key)) {
            return false;
        }
        if (try table.load().mayContain(key)) return true;
        return false;
    }

    pub fn writeBatch(self: *Self, records: []const WriteBatchRecord) !void {
        for (records) |record| {
            switch (record) {
                .put => |pp| {
                    try self.state.getMemTable().put(pp.key, pp.value);
                },
                .delete => |dd| {
                    // we use "" as the tombstone value
                    try self.state.getMemTable().put(dd, "");
                },
            }
            try self.tryFreeze(self.state.getMemTable().getApproximateSize());
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
        return self.writeBatch(&[_]WriteBatchRecord{.{ .delete = key }});
    }

    fn tryFreeze(self: *Self, estimate_size: usize) !void {
        if (estimate_size < self.options.target_sst_size) {
            return;
        }

        self.state_lock.lockShared();
        // double check
        if (self.state.getMemTable().getApproximateSize() >= self.options.target_sst_size) {
            self.state_lock.unlockShared();
            try self.forceFreezeMemtable();
            return;
        }
        self.state_lock.unlockShared();
    }

    pub fn forceFreezeMemtable(self: *Self) !void {
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
                // user_end.key < sst_begin
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
        defer {
            for (memtable_iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            memtable_iters.deinit();
        }

        // collect memtable iterators
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();

            for (self.state.imm_mem_tables.items) |imm_table| {
                if (!imm_table.load().isEmpty()) {
                    var imm_it = imm_table.load().scan(lower, upper);
                    while (!imm_it.isEmpty()) {
                        std.debug.print("{d}: {s}\n", .{ imm_table.load().id, imm_it.key() });
                        imm_it.next();
                    }
                    if (imm_it.isEmpty()) continue;
                    var sp = try StorageIteratorPtr.create(
                        self.allocator,
                        .{ .mem_iter = imm_it },
                    );
                    errdefer sp.release();
                    try memtable_iters.append(sp);
                }
            }
            if (!self.state.getMemTable().isEmpty()) {
                var mm_it = self.state.getMemTable().scan(lower, upper);
                if (!mm_it.isEmpty()) {
                    var sp = try StorageIteratorPtr.create(
                        self.allocator,
                        .{ .mem_iter = mm_it },
                    );
                    errdefer sp.release();
                    try memtable_iters.append(sp);
                }
            }
        }

        var memtable_merge_iters = try MergeIterators.init(self.allocator, memtable_iters);
        errdefer memtable_merge_iters.deinit();

        // l0_sst
        var l0_iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer {
            for (l0_iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            l0_iters.deinit();
        }

        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.l0_sstables.get().ssts.items) |sst_id| {
                const table_ptr = self.state.sstables.get(sst_id).?;
                const table = table_ptr.load();
                if (rangeOverlap(lower, upper, table.firstKey(), table.lastKey())) {
                    var ss_iter: SsTableIterator = undefined;
                    switch (lower.bound_t) {
                        .included => {
                            ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, table_ptr.clone(), lower.data);
                        },
                        .excluded => {
                            ss_iter = try SsTableIterator.initAndSeekToKey(self.allocator, table_ptr.clone(), lower.data);
                            if (!ss_iter.isEmpty() and std.mem.eql(u8, ss_iter.key(), lower.data)) {
                                try ss_iter.next();
                            }
                        },
                        .unbounded => {
                            ss_iter = try SsTableIterator.initAndSeekToFirst(self.allocator, table_ptr.clone());
                        },
                    }
                    errdefer ss_iter.deinit();
                    if (ss_iter.isEmpty()) {
                        continue;
                    }
                    var sp = try StorageIteratorPtr.create(self.allocator, .{
                        .ss_table_iter = ss_iter,
                    });
                    errdefer sp.release();
                    try l0_iters.append(sp);
                }
            }
        }

        // levels
        var lv_iters = std.ArrayList(StorageIteratorPtr).init(self.allocator);
        defer {
            for (lv_iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            lv_iters.deinit();
        }

        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            for (self.state.levels.items) |l| {
                var lv_ssts = try std.ArrayList(SsTablePtr).initCapacity(self.allocator, l.get().size());
                for (l.get().ssts.items) |sst_id| {
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
                if (sst_concat_iter.isEmpty()) {
                    continue;
                }
                var sp = try StorageIteratorPtr.create(self.allocator, .{
                    .sst_concat_iter = sst_concat_iter,
                });
                errdefer sp.release();
                try lv_iters.append(sp);
            }
        }

        var l0_merge_iter = try MergeIterators.init(self.allocator, l0_iters);
        errdefer l0_merge_iter.deinit();

        var lv_merge_iters = try MergeIterators.init(self.allocator, lv_iters);
        errdefer lv_merge_iters.deinit();

        var iter = try TwoMergeIterator.init(
            try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = memtable_merge_iters }),
            try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = l0_merge_iter }),
        );
        errdefer iter.deinit();

        iter = try TwoMergeIterator.init(
            try StorageIteratorPtr.create(self.allocator, .{ .two_merge_iter = iter }),
            try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = lv_merge_iters }),
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
            if (self.compaction_controller.flushToL0()) {
                try self.state.l0_sstables.get().append(sst_id);
            } else {
                // tiered
                var nl = Level.init(sst_id, self.allocator);
                errdefer nl.deinit();
                try nl.append(sst_id);
                try self.state.levels.append(try LevelPtr.create(self.allocator, nl));
            }
            try self.state.sstables.put(sst.id, try SsTablePtr.create(self.allocator, sst));
        }

        // self.dumpState();
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
            self.terminate.timedWait(50 * std.time.ns_per_ms) catch |err| switch (err) {
                error.Timeout => {},
            };
        }
    }

    fn mustFlushLoop(self: *Self) void {
        self.flushLoop() catch |err| {
            std.log.err("flushLoop failed: {s}", .{@errorName(err)});
        };
    }

    fn triggerCompaction(self: *Self) !void {
        var task: CompactionTask = undefined;
        {
            self.state_lock.lockShared();
            defer self.state_lock.unlockShared();
            if (try self.compaction_controller.generateCompactionTask(&self.state)) |ct| {
                task = ct;
            } else {
                return;
            }
        }
        defer task.deinit();
        var sstables = try self.compact(task);
        defer {
            for (sstables.items) |sst| {
                var p = sst;
                p.release();
            }
            sstables.deinit();
        }
        var output = std.ArrayList(usize).init(self.allocator);
        defer output.deinit();
        for (sstables.items) |sst| {
            try output.append(@intCast(sst.get().sstId()));
        }

        // sst to remove
        var ssts_to_remove = std.ArrayList(SsTablePtr).init(self.allocator);
        defer {
            for (ssts_to_remove.items) |sst| {
                var p = sst;
                p.release();
            }
            ssts_to_remove.deinit();
        }
        {
            var new_sst_ids = std.ArrayList(usize).init(self.allocator);
            defer new_sst_ids.deinit();

            self.state_lock.lock();
            defer self.state_lock.unlock();

            for (sstables.items) |sst| {
                const id: usize = @intCast(sst.get().sstId());
                try new_sst_ids.append(id);
                try self.state.sstables.put(id, sst.clone());
            }

            var file_to_remove = try self.compaction_controller.applyCompactionResult(
                &self.state,
                task,
                output.items,
            );
            defer file_to_remove.deinit();

            for (file_to_remove.items) |id| {
                if (self.state.sstables.fetchRemove(id)) |kv| {
                    try ssts_to_remove.append(kv.value);
                }
            }
        }

        for (ssts_to_remove.items) |sst| {
            const path = try self.pathOfSst(sst.get().sstId());
            defer self.allocator.free(path);
            try std.fs.cwd().deleteFile(path);
        }
        try self.syncDir();
        // self.dumpState();
    }

    fn compactionLoop(self: *Self) !void {
        switch (self.compaction_controller) {
            .no_compaction => {},
            inline else => {
                while (!self.terminate.isSet()) {
                    try self.triggerCompaction();
                    self.terminate.timedWait(50 * std.time.ns_per_ms) catch |err| switch (err) {
                        error.Timeout => {},
                    };
                }
            },
        }
    }

    fn mustCompactionLoop(self: *Self) void {
        self.compactionLoop() catch |err| {
            std.log.err("compactionLoop failed: {s}", .{@errorName(err)});
        };
    }

    pub fn spawnFlushThread(self: *Self) void {
        self.wg.spawnManager(mustFlushLoop, .{self});
    }

    pub fn spawnCompactionThread(self: *Self) void {
        self.wg.spawnManager(mustCompactionLoop, .{self});
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
            l0_sstables = try self.state.l0_sstables.get().dump();
            errdefer l0_sstables.deinit();
            l1_sstables = try self.state.levels.getLast().get().dump();
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
                try self.state.sstables.put(id, sst.clone());
            }

            var l1 = self.state.levels.items[0].get();
            l1.clear();
            l1.ssts.appendSlice(ids.items);

            // state.l0_sstables may change after compaction
            {
                var l0_sstables_map = std.AutoHashMap(usize, struct {}).init(self.allocator);
                defer l0_sstables_map.deinit();

                for (l0_sstables.items) |id| {
                    try l0_sstables_map.put(id, .{});
                }

                var new_l0_sstables = std.ArrayList(usize).init(self.allocator);
                defer new_l0_sstables.deinit();

                var l0 = self.state.l0_sstables.get();
                for (l0.ssts.items) |id| {
                    if (!l0_sstables_map.remove(id)) {
                        try new_l0_sstables.append(id);
                    }
                }
                l0.clear();
                l0.ssts.appendSlice(new_l0_sstables.items);
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
            .simple => |s| {
                return self.compactSimple(s);
            },
            .tiered => |t| {
                return self.compactTiered(t);
            },
        }
    }

    fn compactForceFull(self: *Self, f: ForceFullCompaction) !std.ArrayList(SsTablePtr) {
        const l0_sstables = f.l0_sstables;
        const l1_sstables = f.l1_sstables;

        var l0_iters = try std.ArrayList(StorageIteratorPtr).initCapacity(self.allocator, l0_sstables.items.len);
        defer {
            for (l0_iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            l0_iters.deinit();
        }

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

        var two_merge_iter = try TwoMergeIterator.init(
            try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = l0_merge_iter }),
            try StorageIteratorPtr.create(self.allocator, .{ .sst_concat_iter = l1_sst_iter }),
        );
        errdefer two_merge_iter.deinit();

        var iter = StorageIterator{ .two_merge_iter = two_merge_iter };
        defer iter.deinit();

        return self.compactGenerateSstFromIter(&iter, true);
    }

    fn compactSimple(self: *Self, task: SimpleLeveledCompactionTask) !std.ArrayList(SsTablePtr) {
        if (task.upper_level) |_| {
            var upper_ssts = try std.ArrayList(SsTablePtr).initCapacity(
                self.allocator,
                task.upper_level_sst_ids.items.len,
            );
            var lower_ssts = try std.ArrayList(SsTablePtr).initCapacity(
                self.allocator,
                task.lower_level_sst_ids.items.len,
            );

            self.state_lock.lockShared();
            for (task.upper_level_sst_ids.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                try upper_ssts.append(sst.clone());
            }
            for (task.lower_level_sst_ids.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                try lower_ssts.append(sst.clone());
            }
            self.state_lock.unlockShared();

            var upper_iter = try SstConcatIterator.initAndSeekToFirst(self.allocator, upper_ssts);
            errdefer upper_iter.deinit();

            var lower_iter = try SstConcatIterator.initAndSeekToFirst(self.allocator, lower_ssts);
            errdefer lower_iter.deinit();

            var two_merge_iter = try TwoMergeIterator.init(
                try StorageIteratorPtr.create(self.allocator, .{ .sst_concat_iter = upper_iter }),
                try StorageIteratorPtr.create(self.allocator, .{ .sst_concat_iter = lower_iter }),
            );
            errdefer two_merge_iter.deinit();

            var iter = StorageIterator{ .two_merge_iter = two_merge_iter };
            defer iter.deinit();
            return self.compactGenerateSstFromIter(&iter, task.is_lower_level_bottom);
        } else {
            // compact l0_sstables to l1_sstables
            var upper_iters = try std.ArrayList(StorageIteratorPtr).initCapacity(
                self.allocator,
                task.upper_level_sst_ids.items.len,
            );
            defer {
                for (upper_iters.items) |iter| {
                    var ii = iter;
                    ii.release();
                }
                upper_iters.deinit();
            }

            self.state_lock.lockShared();
            for (task.upper_level_sst_ids.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                const ss_iter = try SsTableIterator.initAndSeekToFirst(self.allocator, sst.clone());
                try upper_iters.append(try StorageIteratorPtr.create(self.allocator, .{
                    .ss_table_iter = ss_iter,
                }));
            }
            self.state_lock.unlockShared();

            var upper_iter = try MergeIterators.init(self.allocator, upper_iters);
            errdefer upper_iter.deinit();

            var lower_ssts = try std.ArrayList(SsTablePtr).initCapacity(
                self.allocator,
                task.lower_level_sst_ids.items.len,
            );

            self.state_lock.lockShared();
            for (task.lower_level_sst_ids.items) |sst_id| {
                const sst = self.state.sstables.get(sst_id).?;
                try lower_ssts.append(sst.clone());
            }
            self.state_lock.unlockShared();
            var lower_iter = try SstConcatIterator.initAndSeekToFirst(self.allocator, lower_ssts);
            errdefer lower_iter.deinit();
            var two_merge_iter = try TwoMergeIterator.init(
                try StorageIteratorPtr.create(self.allocator, .{ .merge_iterators = upper_iter }),
                try StorageIteratorPtr.create(self.allocator, .{ .sst_concat_iter = lower_iter }),
            );
            errdefer two_merge_iter.deinit();

            var iter = StorageIterator{ .two_merge_iter = two_merge_iter };
            defer iter.deinit();

            return self.compactGenerateSstFromIter(&iter, task.is_lower_level_bottom);
        }
    }

    fn compactTiered(self: *Self, task: TieredCompactionTask) !std.ArrayList(SsTablePtr) {
        var iters = try std.ArrayList(StorageIteratorPtr).initCapacity(self.allocator, task.tiers.items.len);
        defer {
            for (iters.items) |iter| {
                var ii = iter;
                ii.release();
            }
            iters.deinit();
        }
        for (task.tiers.items) |lp| {
            var ssts = try std.ArrayList(SsTablePtr).initCapacity(self.allocator, lp.get().size());
            for (lp.get().ssts.items) |sst_id| {
                self.state_lock.lockShared();
                const sst = self.state.sstables.get(sst_id).?;
                self.state_lock.unlockShared();
                try ssts.append(sst.clone());
            }
            var sst_iter = try SstConcatIterator.initAndSeekToFirst(self.allocator, ssts);
            errdefer sst_iter.deinit();
            var sp = try StorageIteratorPtr.create(self.allocator, .{ .sst_concat_iter = sst_iter });
            errdefer sp.release();
            try iters.append(sp);
        }
        var merge_iter = try MergeIterators.init(self.allocator, iters);
        errdefer merge_iter.deinit();

        var iter = StorageIterator{ .merge_iterators = merge_iter };
        defer iter.deinit();

        return self.compactGenerateSstFromIter(&iter, task.bottom_tier_included);
    }

    fn compactGenerateSstFromIter(self: *Self, iter: *StorageIterator, compact_to_bottom_level: bool) !std.ArrayList(SsTablePtr) {
        var builder: SsTableBuilder = try SsTableBuilder.init(self.allocator, self.options.block_size);
        defer builder.deinit();
        var new_ssts = std.ArrayList(SsTablePtr).init(self.allocator);
        while (!iter.isEmpty()) {
            // std.debug.print("write {s} => {s}\n", .{ iter.key(), iter.value() });
            if (compact_to_bottom_level) {
                if (iter.value().len > 0) {
                    try builder.add(iter.key(), iter.value());
                }
            } else {
                try builder.add(iter.key(), iter.value());
            }
            if (builder.estimatedSize() >= self.options.target_sst_size) {
                // reset builder
                defer builder.reset() catch unreachable;
                const sst_id = self.getNextSstId();
                const path = try self.pathOfSst(sst_id);
                defer self.allocator.free(path);
                var sst = try builder.build(sst_id, self.block_cache.clone(), path);
                errdefer sst.deinit();

                var sst_ptr = try SsTablePtr.create(self.allocator, sst);
                errdefer sst_ptr.deinit();

                try new_ssts.append(sst_ptr);
            }
            try iter.next();
        }
        if (builder.estimatedSize() > 0) {
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

    fn dumpState(self: *Self) void {
        std.debug.print("------------- Storage Dump -------------\n", .{});
        std.debug.print("memtable: {d}\n", .{self.state.getMemTable().getApproximateSize()});
        std.debug.print("in_mem_sstables: {d}\n", .{self.state.imm_mem_tables.items.len});
        std.debug.print("l0_sstables: {d}\n", .{self.state.l0_sstables.get().size()});
        for (self.state.l0_sstables.get().ssts.items) |sst_id| {
            std.debug.print("{d} ", .{sst_id});
        }
        std.debug.print("\n", .{});
        for (self.state.levels.items, 1..) |level, i| {
            std.debug.print("level{d}: {d}\n", .{ i, level.get().size() });
            for (level.get().ssts.items) |sst_id| {
                std.debug.print("{d} ", .{sst_id});
            }
            std.debug.print("\n", .{});
        }
    }
};

test "init" {
    defer std.fs.cwd().deleteTree("./tmp/storage/init") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };

    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/init", opts);
    defer storage.deinit();
}

test "put/delete/get" {
    defer std.fs.cwd().deleteTree("./tmp/storage/put") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{
            .simple = .{
                .size_ration_percent = 50,
                .level0_file_num_compaction_trigger = 2,
                .max_levels = 3,
            },
        },
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/put", opts);
    defer storage.deinit();

    for (0..128) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = std.fmt.bufPrint(&kb, "key{d:0>5}", .{i}) catch unreachable;
        const vv = std.fmt.bufPrint(&vb, "val{d:0>5}", .{i}) catch unreachable;
        std.debug.print("put {s} {s}\n", .{ kk, vv });
        try storage.put(kk, vv);
        // try storage.triggerFlush();
        // try storage.triggerCompaction();
    }

    try storage.delete("key00003");
    // try storage.triggerFlush();
    // try storage.triggerCompaction();

    var value: []const u8 = undefined;
    if (try storage.get("key00127", &value)) {
        std.debug.print("key00127: {s}\n", .{value});
        try std.testing.expectEqualStrings("val00127", value);
    }

    if (try storage.get("key00003", &value)) {
        unreachable;
    }

    if (try storage.get("not-exist", &value)) {
        unreachable;
    }
}

test "freeze" {
    defer std.fs.cwd().deleteTree("./tmp/storage/freeze") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
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
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{ .no_compaction = .{} },
    };

    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/flush", opts);
    defer storage.deinit();

    for (0..256) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);
        try storage.triggerFlush();
    }
}

test "scan" {
    defer std.fs.cwd().deleteTree("./tmp/storage/scan") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 2,
        .enable_wal = true,
        .compaction_options = .{ .no_compaction = .{} },
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/scan", opts);
    defer storage.deinit();

    for (0..128) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);
    }

    try storage.triggerFlush();
    // try storage.triggerCompaction();
    storage.dumpState();

    var iter = try storage.scan(
        Bound.init("key00005", .included),
        Bound.init("key00012", .excluded),
    );
    defer iter.deinit();

    while (!iter.isEmpty()) {
        std.debug.print("key: {s} value: {s}\n", .{ iter.key(), iter.value() });
        try iter.next();
    }
}

test "full_compact" {
    defer std.fs.cwd().deleteTree("./tmp/storage/full_compact") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{ .no_compaction = .{} },
    };
    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/full_compact", opts);
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

test "simple_compact" {
    defer std.fs.cwd().deleteTree("./tmp/storage/simple_compact") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{
            .simple = .{
                .size_ration_percent = 50,
                .level0_file_num_compaction_trigger = 2,
                .max_levels = 3,
            },
        },
    };

    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/simple_compact", opts);
    defer storage.deinit();

    for (0..512) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);

        try storage.triggerFlush();
        try storage.triggerCompaction();
    }

    var iter = try storage.scan(
        Bound.init("key00278", .included),
        Bound.init("key00299", .included),
    );
    defer iter.deinit();

    while (!iter.isEmpty()) {
        std.debug.print("key: {s} value: {s}\n", .{ iter.key(), iter.value() });
        try iter.next();
    }
}

test "tiered_compact" {
    defer std.fs.cwd().deleteTree("./tmp/storage/tiered_compact") catch unreachable;
    const opts = StorageOptions{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{
            .tiered = .{
                .num_tiers = 4,
                .max_size_amplification_percent = 75,
                .size_ratio = 50,
                .min_merge_width = 2,
                .max_merge_width = null,
            },
        },
    };

    var storage = try StorageInner.init(std.testing.allocator, "./tmp/storage/tiered_compact", opts);
    defer storage.deinit();
    for (0..512) |i| {
        var kb: [10]u8 = undefined;
        var vb: [10]u8 = undefined;
        const kk = try std.fmt.bufPrint(&kb, "key{d:0>5}", .{i});
        const vv = try std.fmt.bufPrint(&vb, "val{d:0>5}", .{i});
        try storage.put(kk, vv);
        try storage.triggerFlush();
        try storage.triggerCompaction();
    }

    var iter = try storage.scan(
        Bound.init("key00278", .included),
        Bound.init("key00299", .included),
    );
    defer iter.deinit();
    while (!iter.isEmpty()) {
        std.debug.print("key: {s} value: {s}\n", .{ iter.key(), iter.value() });
        try iter.next();
    }
}
