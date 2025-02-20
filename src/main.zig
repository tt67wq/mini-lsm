const std = @import("std");
const MiniLsm = @import("MiniLsm.zig");

pub fn main() !void {
    defer std.fs.cwd().deleteTree("./tmp/miniLsm") catch unreachable;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var db = try MiniLsm.init(allocator, "./tmp/miniLsm", .{
        .block_size = 64,
        .target_sst_size = 256,
        .num_memtable_limit = 10,
        .enable_wal = true,
        .compaction_options = .{
            .simple = .{
                .size_ration_percent = 30,
                .level0_file_num_compaction_trigger = 16,
                .max_levels = 6,
            },
        },
    });
    defer db.deinit() catch unreachable;

    // 插入一些键值对
    std.debug.print("Inserting key-value pairs...\n", .{});
    try db.put("key1", "value1");
    try db.put("key2", "value2");
    try db.put("key3", "value3");

    // 读取键值对
    std.debug.print("Reading key1...\n", .{});
    var value: []const u8 = undefined;
    if (try db.get("key1", &value)) {
        std.debug.print("Value for key1: {s}\n", .{value});
    }

    // 删除键值对
    std.debug.print("Deleting key2...\n", .{});
    try db.del("key2");

    // 批量插入键值对
    std.debug.print("Inserting key-value pairs in bulk...\n", .{});
    try db.writeBatch(&[_]MiniLsm.WriteBatchRecord{
        .{ .put = .{ .key = "batch_key1", .value = "batch_value1" } },
        .{ .put = .{ .key = "batch_key2", .value = "batch_value2" } },
        .{ .delete = "key3" },
    });

    // sync
    std.debug.print("sync...\n", .{});
    try db.sync();

    // scan
    std.debug.print("scan...\n", .{});
    var it = try db.scan(MiniLsm.Bound.init("", .unbounded), MiniLsm.Bound.init("", .unbounded));
    defer it.deinit();
    while (!it.isEmpty()) {
        std.debug.print("key: {s}, value: {s}\n", .{ it.key(), it.value() });
        try it.next();
    }
}
