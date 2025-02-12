const std = @import("std");
const storage = @import("storage.zig");

pub const CompactionTask = union(enum) {
    force_full_compaction: ForceFullCompaction,
    simple: SimpleLeveledCompactionTask,

    pub fn compactToBottomLevel(self: CompactionTask) bool {
        return switch (self) {
            .force_full_compaction => true,
            inline else => false,
        };
    }
};

pub const CompactionOptions = union(enum) {
    no_compaction: struct {},
    simple: SimpleLeveledCompactionOptions,

    pub fn is_no_compaction(self: CompactionOptions) bool {
        return switch (self) {
            .no_compaction => true,
            inline else => false,
        };
    }
};

pub const CompactionController = struct {
    no_compaction: struct {},
    simple: SimpleLeveledCompactionController,
};

// ------------------ force full compaction ------------------
pub const ForceFullCompaction = struct {
    l0_sstables: std.ArrayList(usize),
    l1_sstables: std.ArrayList(usize),

    pub fn deinit(self: *ForceFullCompaction) void {
        self.l0_sstables.deinit();
        self.l1_sstables.deinit();
    }
};

// ------------------ simple leveled compaction ------------------
const SimpleLeveledCompactionOptions = struct {
    size_ration_percent: usize,
    level0_file_num_compaction_trigger: usize,
    max_levels: usize,
};

const SimpleLeveledCompactionTask = struct {
    upper_level: ?usize,
    upper_level_sst_ids: std.ArrayList(usize),
    lower_level: usize,
    lower_level_sst_ids: std.ArrayList(usize),
    is_lower_level_bottom: bool,

    pub fn deinit(self: *SimpleLeveledCompactionTask) void {
        self.upper_level_sst_ids.deinit();
        self.lower_level_sst_ids.deinit();
    }
};

const SimpleLeveledCompactionController = struct {
    options: SimpleLeveledCompactionOptions,

    pub fn init(options: SimpleLeveledCompactionOptions) SimpleLeveledCompactionController {
        return .{
            .options = options,
        };
    }

    pub fn generateCompationTask(self: SimpleLeveledCompactionController, state: *storage.StorageState) !?SimpleLeveledCompactionTask {
        if (self.options.max_levels == 1) {
            return null;
        }

        var level_sizes = std.ArrayList(usize).init(state.allocator);
        defer level_sizes.deinit();

        try level_sizes.append(state.l0_sstables.items.len);
        for (state.levels.items) |level| {
            try level_sizes.append(level.items.len);
        }

        // check level0_file_num_compaction_trigger for compaction of L0 to L1
        if (state.l0_sstables.items.len >= self.options.level0_file_num_compaction_trigger) {
            std.log.info("compaction of L0 to L1 because L0 has {d} SSTS >= {d}\n", .{ state.l0_sstables.items.len, self.options.level0_file_num_compaction_trigger });
            return .{
                .upper_level = null,
                .upper_level_sst_ids = try state.l0_sstables.clone(),
                .lower_level = 1,
                .lower_level_sst_ids = try state.levels.items[0].clone(),
                .is_lower_level_bottom = false,
            };
        }

        // check size_ration_percent for compaction of Ln to Ln+1
        for (1..self.options.max_levels) |level| {
            const lower_level = level + 1;
            const size_ration = level_sizes.items[lower_level] * 100 / level_sizes.items[level];
            if (size_ration < self.options.size_ration_percent) {
                std.log.info("compaction of L{d} to L{d} because L{d} size ratio {d} < {d}\n", .{ level, lower_level, level, size_ration, self.options.size_ration_percent });
                return .{
                    .upper_level = level,
                    .upper_level_sst_ids = try state.levels.items[level - 1].clone(),
                    .lower_level = lower_level,
                    .lower_level_sst_ids = try state.levels.items[lower_level - 1].clone(),
                    .is_lower_level_bottom = lower_level == self.options.max_levels,
                };
            }
        }

        return null;
    }

    pub fn applyCompactionResult(
        _: SimpleLeveledCompactionController,
        state: *storage.StorageState,
        task: SimpleLeveledCompactionTask,
        output: []usize,
    ) !std.ArrayList(usize) {
        var files_to_remove = std.ArrayList(usize).init(state.allocator);
        errdefer files_to_remove.deinit();

        if (task.upper_level) |upper_level| {
            try files_to_remove.appendSlice(task.upper_level_sst_ids.items);
            state.levels.items[upper_level - 1].clearAndFree();
        } else {
            try files_to_remove.appendSlice(task.upper_level_sst_ids.items);
            var new_l0_sstables = std.ArrayList(usize).init(state.allocator);
            errdefer new_l0_sstables.deinit();

            {
                var l0_sst_compacted = std.AutoHashMap(usize, struct {});
                defer l0_sst_compacted.deinit();
                for (task.upper_level_sst_ids.items) |sst_id| {
                    try l0_sst_compacted.put(sst_id, .{});
                }

                for (state.l0_sstables.items) |sst_id| {
                    if (!l0_sst_compacted.remove(sst_id)) {
                        try new_l0_sstables.append(sst_id);
                    }
                }
                std.debug.assert(l0_sst_compacted.count() == 0);
            }
            state.l0_sstables.deinit();
            state.l0_sstables = new_l0_sstables;
        }

        try files_to_remove.appendSlice(task.lower_level_sst_ids.items);
        state.levels.items[task.lower_level - 1].clearAndFree();
        try state.levels.items[task.lower_level - 1].appendSlice(output);

        return files_to_remove;
    }
};
