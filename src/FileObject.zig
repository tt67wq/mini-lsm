const std = @import("std");
const fs = std.fs;

const Self = @This();

file: std.fs.File,
size: usize,

pub fn init(path: []const u8, data: []const u8) !Self {
    try fs.cwd().writeFile(.{
        .sub_path = path,
        .data = data,
    });

    var f = try fs.cwd().openFile(path, .{ .mode = .read_only });
    try f.sync();

    return .Self{
        .file = f,
        .size = data.len,
    };
}

pub fn open(path: []const u8) !Self {
    var f = try fs.cwd().openFile(path, .{ .mode = .read_only });
    const md = try f.metadata();
    return .Self{
        .file = f,
        .size = md.size(),
    };
}

pub fn deinit(self: Self) void {
    self.file.close();
}

pub fn read(self: Self, offset: u64, buf: []u8) !usize {
    return try self.file.pread(buf, offset);
}
