const std = @import("std");

pub fn main() !void {
    for (0..10) |i| {
        std.debug.print("{d} ", .{i});
    }
}
