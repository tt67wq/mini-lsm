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
    options: StorageOptions,

    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: StorageOptions) Self {
        var state = StorageState.init(allocator, options);

        // manifest
        const manifest_path = std.fs.path.joinZ(allocator, &[_][]const u8{
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
            std.debug.print("recover from manifest: {s}\n", .{manifest_path});
            if (options.enable_wal) {
                state.mem_table.recover_from_wal() catch {
                    @panic("recover from wal failed");
                };
                if (!state.mem_table.is_empty()) {
                    const old_mm = state.mem_table;
                    state.mem_table = MemTable.init(old_mm.id + 1, allocator, null);
                    state.imm_mem_tables.append(old_mm) catch {
                        @panic("append memtable failed");
                    };
                }
            }
        } else {
            if (options.enable_wal) {
                var old_mm = state.mem_table;
                state.mem_table = MemTable.init(old_mm.id, allocator, manifest_path);
                old_mm.deinit();
            }
        }
        return Self{
            .allocator = allocator,
            .state = state,
            .state_lock = .{},
            .next_sst_id = atomic.Value(usize).init(state.mem_table.id + 1),
            .options = options,
        };
    }

    pub fn deinit(self: *Self) void {
        self.state.deinit();
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
        for (self.state.imm_mem_tables.items) |m| {
            var imm_table = m;
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
        self.state_lock.lock();
        defer self.state_lock.unlock();

        for (records) |record| {
            switch (record) {
                .put => |pp| {
                    self.state.mem_table.put(pp.key, pp.value) catch |err| {
                        std.log.err("put failed: {s}", .{@errorName(err)});
                        @panic("put failed");
                    };
                },
                .delete => |dd| {
                    // we use "" as the tombstone value
                    self.state.mem_table.put(dd, "") catch |err| {
                        std.log.err("delete failed: {s}", .{@errorName(err)});
                        @panic("delete failed");
                    };
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
