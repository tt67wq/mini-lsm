const std = @import("std");
const MemTable = @import("memtable.zig");
const atomic = std.atomic;

pub const StorageOptions = struct {
    block_size: usize,
    target_sst_size: usize,
    num_memtable_limit: usize,
    enable_wal: bool,
};

pub const StorageState = struct {
    allocator: std.mem.Allocator,
    mem_table: MemTable,
    imm_mem_tables: std.ArrayList(MemTable),

    pub fn init(allocator: std.mem.Allocator, _: StorageOptions) StorageState {
        return StorageState{
            .allocator = allocator,
            .mem_table = MemTable.init(0, allocator, null),
            .imm_mem_tables = std.ArrayList(MemTable).init(allocator),
        };
    }

    pub fn deinit(self: *StorageState) void {
        self.mem_table.deinit();
        for (self.imm_mem_tables.items) |m| {
            var imm_table = m;
            imm_table.sync_wal();
            imm_table.deinit();
        }
        self.imm_mem_tables.deinit();
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

    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) Self {
        var state = StorageState.init(allocator, options);
        const next_sst_id: usize = 0;

        // manifest
        const manifest_path = std.fs.path.join(allocator, &[_][]const u8{
            path,
            "MANIFEST",
        }) catch unreachable;
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
                var old_mm = state.mem_table;
                const wal_path = path_of_wal(allocator, path, next_sst_id);
                defer allocator.free(wal_path);
                state.mem_table = MemTable.init(next_sst_id, allocator, wal_path);
                old_mm.deinit();
            }
        }
        return Self{
            .allocator = allocator,
            .path = path,
            .state = state,
            .state_lock = .{},
            .next_sst_id = atomic.Value(usize).init(next_sst_id + 1),
            .options = options,
        };
    }

    pub fn deinit(self: *Self) void {
        self.state.deinit();
    }

    fn get_next_sst_id(self: *Self) usize {
        return self.next_sst_id.fetchAdd(1, .seq_cst);
    }

    fn path_of_wal(
        allocator: std.mem.Allocator,
        path: []const u8,
        id: usize,
    ) [:0]u8 {
        var buf: [9]u8 = undefined;
        const ww = std.fmt.bufPrint(&buf, "{d:0>5}.wal", .{id}) catch unreachable;
        return std.fs.path.joinZ(allocator, &[_][]const u8{ path, ww }) catch unreachable;
    }

    pub fn get(self: *Self, key: []const u8, value: *[]const u8) bool {
        // search in memtable
        self.state_lock.lockShared();
        defer self.state_lock.unlockShared();
        if (self.state.mem_table.get(key, value)) {
            if (value.*.len == 0) {
                // tomestone
                return false;
            }
            return true;
        }
        // search in imm_memtable
        var i = self.state.imm_mem_tables.items.len - 1;
        while (i >= 0) : (i -= 1) {
            const imm_table = self.state.imm_mem_tables.items[i];
            if (imm_table.get(key, value)) {
                if (value.*.len == 0) {
                    // tomestone
                    return false;
                }
                return true;
            }
        }

        return false;
    }

    pub fn write_batch(self: *Self, records: []const WriteBatchRecord) !void {
        for (records) |record| {
            switch (record) {
                .put => |pp| {
                    self.state.mem_table.put(pp.key, pp.value) catch |err| {
                        std.log.err("put failed: {s}", .{@errorName(err)});
                        @panic("put failed");
                    };
                    self.try_freeze(self.state.mem_table.get_approximate_size());
                },
                .delete => |dd| {
                    // we use "" as the tombstone value
                    self.state.mem_table.put(dd, "") catch |err| {
                        std.log.err("delete failed: {s}", .{@errorName(err)});
                        @panic("delete failed");
                    };
                    self.try_freeze(self.state.mem_table.get_approximate_size());
                },
            }
        }
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        return self.write_batch(&[_]WriteBatchRecord{
            .{
                .put = .{
                    .key = key,
                    .value = value,
                },
            },
        });
    }

    pub fn delete(self: *Self, key: []const u8) !void {
        return self.write_batch(&[_]WriteBatchRecord{
            .{
                .delete = key,
            },
        });
    }

    fn try_freeze(self: *Self, estimate_size: usize) void {
        if (estimate_size < self.options.target_sst_size) {
            return;
        }
        {
            self.state_lock.lockShared();
            errdefer self.state_lock.unlockShared();
            // double check
            if (self.state.mem_table.get_approximate_size() >= self.options.target_sst_size) {
                self.state_lock.unlockShared();
                self.force_freeze_memtable();
            }
            self.state_lock.unlockShared();
        }
    }

    fn force_freeze_memtable(self: *Self) !void {
        const next_sst_id = self.get_next_sst_id();
        var new_mm: MemTable = undefined;
        if (self.options.enable_wal) {
            const mm_path = self.path_of_wal(self.allocator, self.path, next_sst_id);
            defer self.allocator.free(mm_path);
            new_mm = MemTable.init(next_sst_id, self.allocator, mm_path);
        } else {
            new_mm = MemTable.init(next_sst_id, self.allocator, null);
        }
        errdefer new_mm.deinit();
        var old_mm: MemTable = undefined;
        {
            self.state_lock.lock();
            defer self.state_lock.unlock();
            old_mm = self.state.mem_table;
            self.state.mem_table = new_mm;
            try self.state.imm_mem_tables.append(old_mm);
        }
        old_mm.sync_wal();
    }
};

test "init" {
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };

    var storage = StorageInner.init(std.testing.allocator, "./tmp/test_storage", opts);
    defer storage.deinit();
}

test "put/delete/get" {
    const opts = StorageOptions{
        .block_size = 1024,
        .target_sst_size = 1024,
        .num_memtable_limit = 10,
        .enable_wal = true,
    };
    var storage = StorageInner.init(std.testing.allocator, "./tmp/test_storage", opts);
    defer storage.deinit();

    try storage.put("key1", "value1");
    try storage.put("key2", "value2");
    try storage.put("key3", "value3");
    try storage.delete("key3");

    var value: []const u8 = undefined;
    if (storage.get("key1", &value)) {
        std.debug.print("key1: {s}\n", .{value});
        try std.testing.expectEqualStrings("value1", value);
    }

    if (storage.get("key2", &value)) {
        std.debug.print("key2: {s}\n", .{value});
        try std.testing.expectEqualStrings("value2", value);
    }

    if (storage.get("key3", &value)) {
        unreachable;
    }
}
