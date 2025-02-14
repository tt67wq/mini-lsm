const std = @import("std");
const MemTable = @import("MemTable.zig");
const ss_table = @import("ss_table.zig");
const MergeIterators = @import("MergeIterators.zig");
const smart_pointer = @import("smart_pointer.zig");
const Bound = @import("MemTable.zig").Bound;
const MemTableIterator = MemTable.MemTableIterator;
const SsTablePtr = ss_table.SsTablePtr;
const SsTableIterator = ss_table.SsTableIterator;

pub const StorageIteratorPtr = smart_pointer.SmartPointer(StorageIterator);
pub const StorageIterator = union(enum) {
    mem_iter: MemTableIterator,
    ss_table_iter: SsTableIterator,
    sst_concat_iter: SstConcatIterator,
    merge_iterators: MergeIterators,
    two_merge_iter: TwoMergeIterator,

    pub fn deinit(self: *StorageIterator) void {
        switch (self.*) {
            .ss_table_iter => self.ss_table_iter.deinit(),
            .sst_concat_iter => self.sst_concat_iter.deinit(),
            .merge_iterators => self.merge_iterators.deinit(),
            .two_merge_iter => self.two_merge_iter.deinit(),
            inline else => {},
        }
    }

    pub fn isEmpty(self: StorageIterator) bool {
        switch (self) {
            inline else => |impl| return impl.isEmpty(),
        }
    }

    pub fn next(self: *StorageIterator) anyerror!void {
        switch (self.*) {
            .mem_iter => self.mem_iter.next(),
            .ss_table_iter => try self.ss_table_iter.next(),
            .sst_concat_iter => try self.sst_concat_iter.next(),
            .merge_iterators => try self.merge_iterators.next(),
            .two_merge_iter => try self.two_merge_iter.next(),
        }
    }

    pub fn key(self: StorageIterator) []const u8 {
        switch (self) {
            inline else => |impl| return impl.key(),
        }
    }

    pub fn value(self: StorageIterator) []const u8 {
        switch (self) {
            inline else => |impl| return impl.value(),
        }
    }

    pub fn numActiveIterators(self: StorageIterator) usize {
        switch (self) {
            inline else => |impl| return impl.numActiveIterators(),
        }
    }
};

pub const TwoMergeIterator = struct {
    a: StorageIteratorPtr,
    b: StorageIteratorPtr,
    choose_a: bool,

    fn chooseA(a: *StorageIterator, b: *StorageIterator) bool {
        if (a.isEmpty()) {
            return false;
        }
        if (b.isEmpty()) {
            return true;
        }
        return std.mem.lessThan(u8, a.key(), b.key());
    }

    fn skipB(self: *TwoMergeIterator) !void {
        const ap = self.a.load();
        const bp = self.b.load();
        if (!ap.isEmpty() and !bp.isEmpty() and std.mem.eql(u8, ap.key(), bp.key())) try bp.next();
    }

    pub fn init(a: StorageIteratorPtr, b: StorageIteratorPtr) !TwoMergeIterator {
        var iter = TwoMergeIterator{
            .a = a,
            .b = b,
            .choose_a = false,
        };
        try iter.skipB();
        iter.choose_a = chooseA(iter.a.load(), iter.b.load());
        return iter;
    }

    pub fn deinit(self: *TwoMergeIterator) void {
        self.a.release();
        self.b.release();
    }

    pub fn key(self: TwoMergeIterator) []const u8 {
        if (self.choose_a) {
            std.debug.assert(!self.a.load().isEmpty());
            return self.a.load().key();
        }
        std.debug.assert(!self.b.load().isEmpty());
        return self.b.load().key();
    }

    pub fn value(self: TwoMergeIterator) []const u8 {
        if (self.choose_a) {
            std.debug.assert(!self.a.load().isEmpty());
            return self.a.load().value();
        }
        std.debug.assert(!self.b.load().isEmpty());
        return self.b.load().value();
    }

    pub fn isEmpty(self: TwoMergeIterator) bool {
        if (self.choose_a) {
            return self.a.load().isEmpty();
        }
        return self.b.load().isEmpty();
    }

    pub fn next(self: *TwoMergeIterator) !void {
        if (self.choose_a) {
            try self.a.load().next();
        } else {
            try self.b.load().next();
        }
        try self.skipB();
        self.choose_a = chooseA(self.a.load(), self.b.load());
    }

    pub fn numActiveIterators(self: TwoMergeIterator) usize {
        return self.a.load().numActiveIterators() + self.b.load().numActiveIterators();
    }
};

const LsmIteratorInner = TwoMergeIterator;

pub const LsmIterator = struct {
    inner: LsmIteratorInner,
    end_bound: Bound,
    is_empty: bool,

    pub fn init(
        inner: LsmIteratorInner,
        end_bound: Bound,
    ) LsmIterator {
        return LsmIterator{
            .inner = inner,
            .end_bound = end_bound,
            .is_empty = inner.isEmpty(),
        };
    }

    pub fn deinit(self: *LsmIterator) void {
        self.inner.deinit();
    }

    fn nextInner(self: *LsmIterator) !void {
        try self.inner.next();
        if (self.inner.isEmpty()) {
            self.is_empty = true;
            return;
        }
        switch (self.end_bound.bound_t) {
            .unbounded => {},
            .included => {
                self.is_empty = std.mem.lessThan(u8, self.key(), self.end_bound.data) or
                    std.mem.eql(u8, self.key(), self.end_bound.data);
            },
            .excluded => {
                self.is_empty = std.mem.lessThan(u8, self.key(), self.end_bound.data);
            },
        }
        return;
    }

    fn moveToNoneDelete(self: *LsmIterator) !void {
        while (!self.isEmpty() and self.inner.value().len == 0) {
            try self.nextInner();
        }
    }

    pub fn next(self: *LsmIterator) !void {
        try self.nextInner();
        try self.moveToNoneDelete();
    }

    pub fn isEmpty(self: LsmIterator) bool {
        return self.is_empty;
    }

    pub fn key(self: LsmIterator) []const u8 {
        return self.inner.key();
    }

    pub fn value(self: LsmIterator) []const u8 {
        return self.inner.value();
    }

    pub fn numActiveIterators(self: LsmIterator) usize {
        return self.inner.numActiveIterators();
    }
};

pub const SstConcatIterator = struct {
    allocator: std.mem.Allocator,
    current: ?SsTableIterator,
    next_sst_idx: usize,
    sstables: std.ArrayList(SsTablePtr),

    const Self = @This();

    pub fn deinit(self: *Self) void {
        if (self.current) |_| {
            self.current.?.deinit();
        }

        for (self.sstables.items) |sst| {
            var p = sst;
            p.deinit();
        }
        self.sstables.deinit();
    }

    fn checkSstValid(sstables: std.ArrayList(SsTablePtr)) void {
        for (sstables.items) |sst| {
            const fk = sst.get().first_key;
            const lk = sst.get().last_key;
            std.debug.assert(std.mem.lessThan(u8, fk, lk) or std.mem.eql(u8, fk, lk));
        }
        if (sstables.items.len > 2) {
            for (0..sstables.items.len - 2) |idx| {
                std.debug.assert(
                    std.mem.lessThan(u8, sstables.items[idx].get().last_key, sstables.items[idx + 1].get().first_key),
                );
            }
        }
    }

    pub fn initAndSeekToFirst(allocator: std.mem.Allocator, sstables: std.ArrayList(SsTablePtr)) !Self {
        checkSstValid(sstables);
        if (sstables.items.len == 0) {
            return .{
                .allocator = allocator,
                .current = null,
                .next_sst_idx = 0,
                .sstables = sstables,
            };
        }

        var ss_iter = try SsTableIterator.initAndSeekToFirst(allocator, sstables.items[0].clone());
        errdefer ss_iter.deinit();

        var iter = Self{
            .allocator = allocator,
            .current = ss_iter,
            .next_sst_idx = 1,
            .sstables = sstables,
        };
        try iter.moveUntilValid();
        return iter;
    }

    pub fn initAndSeekToKey(allocator: std.mem.Allocator, sstables: std.ArrayList(SsTablePtr), k: []const u8) !Self {
        checkSstValid(sstables);
        if (sstables.items.len == 0) {
            return .{
                .allocator = allocator,
                .current = null,
                .next_sst_idx = 0,
                .sstables = sstables,
            };
        }
        // binary search
        var index: usize = 0;
        {
            var left: usize = 0;
            var right: usize = sstables.items.len - 1;
            while (left <= right) {
                const mid = left + (right - left) / 2;
                const sst = sstables.items[mid].get();
                const fk = sst.first_key;
                const lk = sst.last_key;

                if (std.mem.lessThan(u8, k, fk)) {
                    right = mid - 1;
                } else if (std.mem.lessThan(u8, lk, k)) {
                    left = mid + 1;
                } else {
                    index = mid;
                    break;
                }
            }
            // not found
            index = sstables.items.len;
        }
        if (index >= sstables.items.len) {
            return .{
                .allocator = allocator,
                .current = null,
                .next_sst_idx = sstables.items.len,
                .sstables = sstables,
            };
        }
        var ss_iter = try SsTableIterator.initAndSeekToKey(allocator, sstables.items[index].clone(), k);
        errdefer ss_iter.deinit();

        var iter = Self{
            .allocator = allocator,
            .current = ss_iter,
            .next_sst_idx = index + 1,
            .sstables = sstables,
        };

        try iter.moveUntilValid();
        return iter;
    }

    fn moveUntilValid(self: *Self) !void {
        while (self.current) |iter| {
            if (!iter.isEmpty()) {
                return;
            }
            // current is empty
            self.current.?.deinit();
            if (self.next_sst_idx >= self.sstables.items.len) {
                self.current = null;
            } else {
                const ss_iter = try SsTableIterator.initAndSeekToFirst(
                    self.allocator,
                    self.sstables.items[self.next_sst_idx].clone(),
                );
                self.current = ss_iter;
                self.next_sst_idx += 1;
            }
        }
    }

    pub fn isEmpty(self: Self) bool {
        if (self.current) |iter| {
            return iter.isEmpty();
        }
        return true;
    }

    pub fn key(self: Self) []const u8 {
        return self.current.?.key();
    }

    pub fn value(self: Self) []const u8 {
        return self.current.?.value();
    }

    pub fn next(self: *Self) !void {
        try self.current.?.next();
        try self.moveUntilValid();
    }

    pub fn numActiveIterators(_: Self) usize {
        return 1;
    }
};

test "sst concat iterator" {
    defer {
        std.fs.cwd().deleteTree("./tmp/ss_concat_iter") catch {
            std.debug.panic("delete tmp dir failed", .{});
        };
    }
    try std.fs.cwd().makePath("./tmp/ss_concat_iter");

    const SsTableBuilder = ss_table.SsTableBuilder;

    var ssts = std.ArrayList(SsTablePtr).init(std.testing.allocator);
    for (0..4) |i| {
        var sb = try SsTableBuilder.init(std.testing.allocator, 256);
        defer sb.deinit();

        for (0..16) |j| {
            var kb: [64]u8 = undefined;
            var vb: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&kb, "key{:0>5}", .{i * 100 + j});
            const value = try std.fmt.bufPrint(&vb, "value{:0>5}", .{i * 100 + j});
            try sb.add(key, value);
        }
        const path = try std.fmt.allocPrint(std.testing.allocator, "./tmp/ss_concat_iter/{d:0>5}", .{i});
        defer std.testing.allocator.free(path);
        const table = try sb.build(i, null, path);
        var ssp = try SsTablePtr.create(std.testing.allocator, table);
        errdefer ssp.deinit();
        try ssts.append(ssp);
    }

    var iter = try SstConcatIterator.initAndSeekToFirst(std.testing.allocator, ssts);
    defer iter.deinit();

    while (!iter.isEmpty()) {
        std.debug.print("{s} {s}\n", .{ iter.key(), iter.value() });
        try iter.next();
    }
}

test "two merge iterator" {
    defer {
        std.fs.cwd().deleteTree("./tmp/two_merge_iter") catch {
            std.debug.panic("delete tmp dir failed", .{});
        };
    }
    try std.fs.cwd().makePath("./tmp/two_merge_iter");

    const SsTableBuilder = ss_table.SsTableBuilder;

    var ssts1 = std.ArrayList(SsTablePtr).init(std.testing.allocator);
    var ssts2 = std.ArrayList(SsTablePtr).init(std.testing.allocator);
    for (0..4) |i| {
        var sb = try SsTableBuilder.init(std.testing.allocator, 256);
        defer sb.deinit();

        for (0..16) |j| {
            var kb: [64]u8 = undefined;
            var vb: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&kb, "key{:0>5}", .{i * 100 + j});
            const value = try std.fmt.bufPrint(&vb, "value{:0>5}", .{i * 100 + j});
            try sb.add(key, value);
        }
        const path = try std.fmt.allocPrint(std.testing.allocator, "./tmp/two_merge_iter/{d:0>5}", .{i});
        defer std.testing.allocator.free(path);
        const table = try sb.build(i, null, path);
        var ssp = try SsTablePtr.create(std.testing.allocator, table);
        errdefer ssp.deinit();
        try ssts1.append(ssp);
    }

    for (5..9) |i| {
        var sb = try SsTableBuilder.init(std.testing.allocator, 256);
        defer sb.deinit();

        for (0..16) |j| {
            var kb: [64]u8 = undefined;
            var vb: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&kb, "key{:0>5}", .{i * 100 + j});
            const value = try std.fmt.bufPrint(&vb, "value{:0>5}", .{i * 100 + j});
            try sb.add(key, value);
        }
        const path = try std.fmt.allocPrint(std.testing.allocator, "./tmp/two_merge_iter/{d:0>5}", .{i});
        defer std.testing.allocator.free(path);
        const table = try sb.build(i, null, path);
        var ssp = try SsTablePtr.create(std.testing.allocator, table);
        errdefer ssp.deinit();
        try ssts2.append(ssp);
    }

    var iter1 = try SstConcatIterator.initAndSeekToFirst(std.testing.allocator, ssts1);
    errdefer iter1.deinit();
    var iter2 = try SstConcatIterator.initAndSeekToFirst(std.testing.allocator, ssts2);
    errdefer iter2.deinit();

    var iter = try TwoMergeIterator.init(
        try StorageIteratorPtr.create(std.testing.allocator, .{ .sst_concat_iter = iter2 }),
        try StorageIteratorPtr.create(std.testing.allocator, .{ .sst_concat_iter = iter1 }),
    );
    defer iter.deinit();
    while (!iter.isEmpty()) {
        std.debug.print("{s} {s}\n", .{ iter.key(), iter.value() });
        try iter.next();
    }
}
