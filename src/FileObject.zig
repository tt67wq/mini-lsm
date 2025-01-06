const std = @import("std");
const fs = std.fs;

const Self = @This();

file: ?std.fs.File,
size: usize,

pub fn init(path: []const u8, data: []const u8) !Self {
    try fs.cwd().writeFile(.{
        .sub_path = path,
        .data = data,
    });

    var f = try fs.cwd().openFile(path, .{ .mode = .read_only });
    try f.sync();

    return .{
        .file = f,
        .size = data.len,
    };
}

pub fn open(path: []const u8) !Self {
    var f = try fs.cwd().openFile(path, .{ .mode = .read_only });
    const md = try f.metadata();
    return .{
        .file = f,
        .size = md.size(),
    };
}

pub fn deinit(self: Self) void {
    if (self.file) |f| f.close();
}

pub fn read(self: Self, offset: u64, buf: []u8) !usize {
    return try self.file.?.preadAll(buf, offset);
}

pub fn reader(self: Self) fs.File.Reader {
    return self.file.?.reader();
}

test "file" {
    var f = try Self.init("./tmp/test.txt", "hello world");
    defer f.deinit();

    var buf: [11]u8 = undefined;
    const sz = try f.read(0, buf[0..]);
    try std.testing.expectEqual(11, sz);
    try std.testing.expectEqualSlices(u8, "hello world", buf[0..]);

    var f2 = try Self.open("./tmp/test.txt");
    defer f2.deinit();

    var buf2: [5]u8 = undefined;
    const sz2 = try f2.read(6, buf2[0..]);
    try std.testing.expectEqual(5, sz2);
    try std.testing.expectEqualSlices(u8, "world", buf2[0..]);
}
