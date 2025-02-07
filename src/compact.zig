const std = @import("std");

pub const ForceFullCompaction = struct {
    l0_sstables: std.ArrayList(usize),
    l1_sstables: std.ArrayList(usize),

    pub fn deinit(self: *ForceFullCompaction) void {
        self.l0_sstables.deinit();
        self.l1_sstables.deinit();
    }
};

pub const CompactionTask = union(enum) {
    force_full_compaction: ForceFullCompaction,

    pub fn compactToBottomLevel(self: CompactionTask) bool {
        return switch (self) {
            .force_full_compaction => true,
            inline else => false,
        };
    }
};

pub const CompactionOptions = union(enum) {
    no_compaction: void,

    pub fn is_no_compaction(self: CompactionOptions) bool {
        return switch (self) {
            .no_compaction => true,
            inline else => false,
        };
    }
};
