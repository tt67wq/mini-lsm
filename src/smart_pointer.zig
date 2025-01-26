const std = @import("std");
const atomic = std.atomic;
const testing = std.testing;

// 第 1 阶段：定义基础结构
const RefCounted = struct {
    allocator: std.mem.Allocator,
    counter: atomic.Value(usize),
    data: *anyopaque,
    destroy_fn: *const fn (std.mem.Allocator, *anyopaque) void,

    // 创建智能指针（基础版本）
    fn create(allocator: std.mem.Allocator, ptr: anytype, destroy: fn (std.mem.Allocator, @TypeOf(ptr)) void) !*RefCounted {
        const wrapper = try allocator.create(RefCounted);

        wrapper.* = .{
            .allocator = allocator,
            .counter = atomic.Value(usize).init(1),
            .data = ptr,
            .destroy_fn = @ptrCast(&destroy),
        };

        return wrapper;
    }

    // 增加引用计数
    fn retain(rc: *RefCounted) void {
        _ = rc.counter.fetchAdd(1, .monotonic);
    }

    // 减少引用计数（基础版本）
    fn release(rc: *RefCounted) void {
        const prev = rc.counter.fetchSub(1, .release);
        if (prev == 1) {
            rc.counter.fence(.acquire);
            rc.destroy_fn(rc.allocator, rc.data);
            rc.allocator.destroy(rc);
        }
    }
};

// 第 2 阶段：类型安全包装器
pub fn SmartPointer(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        rc: *RefCounted,
        ptr: *T,

        const Self = @This();

        // 创建智能指针（类型安全版本）
        pub fn create(allocator: std.mem.Allocator, value: T) !Self {
            const ptr = try allocator.create(T);
            ptr.* = value;

            return .{
                .allocator = allocator,
                .rc = try RefCounted.create(allocator, ptr, destroyT),
                .ptr = ptr,
            };
        }

        // 类型特化的销毁函数
        fn destroyT(allocator: std.mem.Allocator, ptr: *T) void {
            mayDeinit(ptr);
            allocator.destroy(ptr);
        }

        fn mayDeinit(ptr: *T) void {
            const typeinfo = @typeInfo(T);
            if (typeinfo != .Struct) return;
            if (@hasDecl(T, "deinit")) {
                ptr.deinit();
            }
        }

        // 复制指针（增加引用计数）
        pub fn clone(self: Self) Self {
            self.rc.retain();
            return .{
                .allocator = self.allocator,
                .rc = self.rc,
                .ptr = self.ptr,
            };
        }

        // 释放资源
        pub fn release(self: *Self) void {
            self.rc.release();
            self.ptr = undefined;
        }
    };
}

// 增强安全性（编译时类型检查）
pub fn get(comptime T: type, sp: anytype) *T {
    if (@TypeOf(sp.ptr) != *T) {
        @compileError("Type mismatch in smart pointer access");
    }
    return sp.ptr;
}

test "智能指针基础功能" {
    const MyType = struct { value: u32 };

    // 创建初始指针
    var sp = try SmartPointer(MyType).create(std.testing.allocator, .{ .value = 42 });
    // defer sp.release();

    try testing.expect(sp.ptr.value == 42);
    try testing.expect(sp.rc.counter.load(.monotonic) == 1);

    // 创建副本
    var sp2 = sp.clone();
    defer sp2.release();

    try testing.expect(sp.rc.counter.load(.monotonic) == 2);

    // 修改原始数据
    sp.ptr.value += 10;
    sp.release();
    try testing.expect(sp2.ptr.value == 52);
}

test "Struct with deinit" {
    const MyType = struct {
        allocator: std.mem.Allocator,
        value: []u8,

        const Self = @This();

        fn init(allocator: std.mem.Allocator, value: []const u8) !Self {
            return .{
                .allocator = allocator,
                .value = try allocator.dupe(u8, value),
            };
        }

        fn deinit(self: *Self) void {
            self.allocator.free(self.value);
        }
    };

    const m1 = try MyType.init(std.testing.allocator, "Hello");
    var sp = try SmartPointer(MyType).create(std.testing.allocator, m1);
    defer sp.release();
}

test "类型安全访问" {
    const FloatPtr = SmartPointer(f32);
    var sp = try FloatPtr.create(std.testing.allocator, 3.14);
    defer sp.release();

    // 正确访问
    _ = get(f32, sp);

    // 以下代码会在编译时报错
    // _ = get(u32, sp);
}

test "并发引用计数" {
    const Concurrency = 100;
    const TestData = struct { value: u32 };

    var sp = try SmartPointer(TestData).create(std.testing.allocator, .{ .value = 0 });
    defer sp.release();

    var threads: [Concurrency]std.Thread = undefined;

    // 创建并发增加引用计数的线程
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn func(s: *SmartPointer(TestData)) void {
                var local_copy = s.clone();
                defer local_copy.release();
            }
        }.func, .{&sp});
    }

    // 等待所有线程完成
    for (threads) |t| t.join();

    // 最终引用计数应为1（初始计数 + N线程增加 - N线程释放）
    try testing.expect(sp.rc.counter.load(.monotonic) == 1);
}
