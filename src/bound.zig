pub const BoundType = enum {
    unbounded,
    included,
    excluded,
};

pub fn Bound(comptime T: type) type {
    return struct {
        data: T,
        bound_t: BoundType,

        const Self = @This();

        pub fn init(data: T, t: BoundType) Self {
            return .{
                .data = data,
                .bound_t = t,
            };
        }

        pub fn isUnbounded(self: Self) bool {
            return self.bound_t == BoundType.unbounded;
        }
    };
}
