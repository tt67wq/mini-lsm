const std = @import("std");
const atomic = std.atomic;
const testing = std.testing;

// 第 1 阶段：定义基础结构
const RefCounted = struct {
    allocator: std.mem.Allocator,
    counter: atomic.Value(usize),
    data: *anyopaque,
    destroy_fn: *const fn (*anyopaque) void,

    // 创建智能指针（基础版本）
    fn create(allocator: std.mem.Allocator, ptr: anytype, destroy: fn (@TypeOf(ptr)) void) !*RefCounted {
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
            rc.destroy_fn(rc.data);
            rc.allocator.destroy(rc);
        }
    }
};

// 第 2 阶段：类型安全包装器
fn SmartPointer(comptime T: type) type {
    return struct {
        rc: *RefCounted,
        ptr: *T,

        const Self = @This();

        // 创建智能指针（类型安全版本）
        fn create(allocator: std.mem.Allocator, value: T) !Self {
            const ptr = try std.heap.page_allocator.create(T);
            ptr.* = value;

            return .{
                .rc = try RefCounted.create(allocator, ptr, destroyT),
                .ptr = ptr,
            };
        }

        // 类型特化的销毁函数
        fn destroyT(ptr: *T) void {
            std.heap.page_allocator.destroy(ptr);
        }

        // 复制指针（增加引用计数）
        fn clone(self: Self) Self {
            self.rc.retain();
            return .{
                .rc = self.rc,
                .ptr = self.ptr,
            };
        }

        // 释放资源
        fn release(self: *Self) void {
            self.rc.release();
            self.ptr = undefined;
        }
    };
}

// 增强安全性（编译时类型检查）
fn get(comptime T: type, sp: anytype) *T {
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
