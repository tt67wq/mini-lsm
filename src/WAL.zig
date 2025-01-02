const std = @import("std");
const swal = @cImport({
    @cInclude("swal.h");
});
const Self = @This();
const WalError = error{
    WalOpenFailed,
    WalAppendFailed,
    WalReplayFailed,
    WalSyncFailed,
};

options: *swal.swal_options_t = undefined,
wal: *swal.swal_t = undefined,

pub fn init(directory: []const u8) !Self {
    // prepare directory
    std.fs.cwd().makePath(directory) catch |err| {
        std.log.err("Failed to make path {s}: {any}", .{ directory, err });
        return WalError.WalOpenFailed;
    };

    const options = swal.swal_options_create();
    var wal: ?*swal.swal_t = undefined;

    const ret = swal.swal_open(@ptrCast(directory), options, &wal);
    if (ret != 0) {
        std.log.err("Failed to open wal {s}: {d}", .{ directory, ret });
        return WalError.WalOpenFailed;
    }
    return Self{
        .options = options,
        .wal = wal.?,
    };
}

pub fn deinit(self: Self) void {
    _ = swal.swal_close(self.wal);
    swal.swal_options_destroy(self.options);
}

pub fn append(self: Self, data: []const u8) WalError!void {
    if (swal.swal_append(self.wal, data.ptr, data.len) < 0) {
        return WalError.WalAppendFailed;
    }
}

pub fn sync(self: Self) WalError!void {
    if (swal.swal_sync(self.wal) != 0) {
        return WalError.WalSyncFailed;
    }
}

pub fn syncMeta(self: Self) WalError!void {
    if (swal.swal_sync_meta(self.wal) != 0) {
        return WalError.WalSyncFailed;
    }
}

pub fn replay(self: Self, offset: usize, limit: i64, reply_func: swal.swal_replay_logfunc, data: ?*anyopaque) WalError!void {
    if (swal.swal_replay(self.wal, offset, limit, reply_func, data) != 0) {
        return WalError.WalReplayFailed;
    }
}

pub fn reset(self: Self, offset: usize) void {
    _ = swal.swal_reset(self.wal, offset, 0);
}

pub fn startOffset(self: Self) usize {
    return swal.swal_start_offset(self.wal);
}

pub fn endOffset(self: Self) usize {
    return swal.swal_end_offset(self.wal);
}

test "wal" {
    defer std.fs.cwd().deleteTree("./tmp/wal") catch unreachable;
    var wal = try Self.init("./tmp/wal");
    defer wal.deinit();
    for (0..100) |i| {
        var buf: [10]u8 = undefined;
        const ret = try std.fmt.bufPrint(&buf, "Hello {d}", .{i});
        try wal.append(ret);
    }
    try wal.sync();
    try wal.syncMeta();

    const replyer = struct {
        pub fn reply(log: ?*const anyopaque, size: usize, _: ?*anyopaque) callconv(.C) usize {
            var content: []const u8 = undefined;
            content.ptr = @ptrCast(log.?);
            std.debug.print("size: {d}, log: {s}", .{ size, content[0..size] });
            std.debug.print("\n", .{});
            return 0;
        }
    };

    try wal.replay(0, 100, replyer.reply, null);
    try wal.replay(400, -1, replyer.reply, null);
}

test "reopen wal" {
    defer std.fs.cwd().deleteTree("./tmp/reopen_wal") catch unreachable;
    var wal = try Self.init("./tmp/reopen_wal");
    for (0..100) |i| {
        var buf: [10]u8 = undefined;
        const ret = try std.fmt.bufPrint(&buf, "Hello {d}", .{i});
        try wal.append(ret);
    }
    try wal.sync();
    try wal.syncMeta();

    wal.deinit();

    const replyer = struct {
        pub fn reply(log: ?*const anyopaque, size: usize, _: ?*anyopaque) callconv(.C) usize {
            var content: []const u8 = undefined;
            content.ptr = @ptrCast(log.?);
            std.debug.print("size: {d}, log: {s}", .{ size, content[0..size] });
            std.debug.print("\n", .{});
            return 0;
        }
    };

    // reopen
    wal = try Self.init("./tmp/reopen_wal");
    try wal.replay(0, 100, replyer.reply, null);
    try wal.replay(20, -1, replyer.reply, null);
    try wal.replay(0, -1, replyer.reply, null);
    wal.deinit();
}

test "reset" {
    defer std.fs.cwd().deleteTree("./tmp/reset_wal") catch unreachable;
    var wal = try Self.init("./tmp/reset_wal");
    for (0..100) |i| {
        var buf: [10]u8 = undefined;
        const ret = try std.fmt.bufPrint(&buf, "Hello {d}", .{i});
        try wal.append(ret);
    }
    try wal.sync();
    try wal.syncMeta();

    std.debug.print("start offset: {d}\n", .{wal.startOffset()});
    std.debug.print("end offset: {d}\n", .{wal.endOffset()});

    wal.reset(700);

    std.debug.print("start offset: {d}\n", .{wal.startOffset()});
    std.debug.print("end offset: {d}\n", .{wal.endOffset()});
}
