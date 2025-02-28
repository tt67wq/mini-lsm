const std = @import("std");
const storage = @import("storage.zig");
const sstable = @import("ss_table.zig");

pub const CompactionTask = union(enum) {
    force_full_compaction: ForceFullCompaction,
    simple: SimpleLeveledCompactionTask,
    tiered: TieredCompactionTask,
    leveled: LeveledCompactionTask,

    pub fn deinit(self: *CompactionTask) void {
        switch (self.*) {
            .force_full_compaction => self.force_full_compaction.deinit(),
            .simple => self.simple.deinit(),
            .tiered => self.tiered.deinit(),
            .leveled => self.leveled.deinit(),
        }
    }
    pub fn compactToBottomLevel(self: CompactionTask) bool {
        return switch (self) {
            .force_full_compaction => true,
            .simple => self.simple.is_lower_level_bottom,
            .tiered => self.tiered.bottom_tier_included,
            .leveled => self.leveled.is_lower_level_bottom,
        };
    }
};

pub const CompactionOptions = union(enum) {
    no_compaction: struct {},
    simple: SimpleLeveledCompactionOptions,
    tiered: TieredCompactionOptions,
    leveled: LeveledCompactionOptions,

    pub fn is_no_compaction(self: CompactionOptions) bool {
        return switch (self) {
            .no_compaction => true,
            inline else => false,
        };
    }
};

pub const CompactionController = union(enum) {
    no_compaction: struct {},
    simple: SimpleLeveledCompactionController,
    tiered: TieredCompactionController,
    leveled: LeveledCompactionController,

    pub fn generateCompactionTask(self: CompactionController, state: *storage.StorageState) !?CompactionTask {
        switch (self) {
            .simple => |controller| {
                if (try controller.generateCompactionTask(state)) |task| return .{ .simple = task };
                return null;
            },
            .tiered => |controller| {
                if (try controller.generateCompactionTask(state)) |task| return .{ .tiered = task };
                return null;
            },
            .leveled => |controller| {
                if (try controller.generateCompactionTask(state)) |task| return .{ .leveled = task };
                return null;
            },
            inline else => unreachable,
        }
    }

    pub fn applyCompactionResult(
        self: CompactionController,
        state: *storage.StorageState,
        task: CompactionTask,
        output: []usize,
        in_recovery: bool,
    ) !std.ArrayList(usize) {
        switch (self) {
            .simple => |controller| {
                return try controller.applyCompactionResult(state, task.simple, output);
            },
            .tiered => |controller| {
                return try controller.applyCompactionResult(state, task.tiered, output);
            },
            .leveled => |controller| {
                return try controller.applyCompactionResult(state, task.leveled, output, in_recovery);
            },
            inline else => unreachable,
        }
    }

    pub fn flushToL0(self: CompactionController) bool {
        switch (self) {
            .tiered => |_| return false,
            inline else => return true,
        }
    }
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

pub const SimpleLeveledCompactionTask = struct {
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

pub const SimpleLeveledCompactionController = struct {
    options: SimpleLeveledCompactionOptions,

    pub fn init(options: SimpleLeveledCompactionOptions) SimpleLeveledCompactionController {
        return .{
            .options = options,
        };
    }

    fn generateCompactionTask(self: SimpleLeveledCompactionController, state: *storage.StorageState) !?SimpleLeveledCompactionTask {
        if (self.options.max_levels == 1) return null;

        // check level0_file_num_compaction_trigger for compaction of L0 to L1
        if (state.l0_sstables.get().size() >= self.options.level0_file_num_compaction_trigger) {
            std.debug.print(
                "compaction of L0 to L1 because L0 has {d} SSTS >= {d}\n",
                .{ state.l0_sstables.get().size(), self.options.level0_file_num_compaction_trigger },
            );
            return .{
                .upper_level = null,
                .upper_level_sst_ids = try state.l0_sstables.get().dump(),
                .lower_level = 1,
                .lower_level_sst_ids = try state.levels.items[0].get().dump(),
                .is_lower_level_bottom = false,
            };
        }

        var level_sizes = std.ArrayList(usize).init(state.allocator);
        defer level_sizes.deinit();

        try level_sizes.append(state.l0_sstables.get().size());
        for (state.levels.items) |level| {
            try level_sizes.append(level.get().size());
        }

        // check size_ration_percent for compaction of Ln to Ln+1
        for (1..self.options.max_levels) |level| {
            const lower_level = level + 1;
            if (level_sizes.items[level] == 0) {
                continue;
            }
            const size_ration = level_sizes.items[lower_level] * 100 / level_sizes.items[level];
            if (size_ration < self.options.size_ration_percent) {
                std.debug.print("compaction of L{d} to L{d} because L{d} size ratio {d} < {d}\n", .{ level, lower_level, level, size_ration, self.options.size_ration_percent });
                return .{
                    .upper_level = level,
                    .upper_level_sst_ids = try state.levels.items[level - 1].get().dump(),
                    .lower_level = lower_level,
                    .lower_level_sst_ids = try state.levels.items[lower_level - 1].get().dump(),
                    .is_lower_level_bottom = lower_level == self.options.max_levels,
                };
            }
        }

        return null;
    }

    fn applyCompactionResult(
        _: SimpleLeveledCompactionController,
        state: *storage.StorageState,
        task: SimpleLeveledCompactionTask,
        output: []usize,
    ) !std.ArrayList(usize) {
        var files_to_remove = std.ArrayList(usize).init(state.allocator);
        errdefer files_to_remove.deinit();

        if (task.upper_level) |upper_level| {
            std.debug.assert(sliceEquals(
                task.upper_level_sst_ids,
                state.levels.items[upper_level - 1].get().ssts,
            ));
            try files_to_remove.appendSlice(task.upper_level_sst_ids.items);
            state.levels.items[upper_level - 1].get().clear();
        } else {
            try files_to_remove.appendSlice(task.upper_level_sst_ids.items);
            var new_l0_sstables = std.ArrayList(usize).init(state.allocator);
            defer new_l0_sstables.deinit();

            {
                var l0_sst_compacted = std.AutoHashMap(usize, struct {}).init(state.allocator);
                defer l0_sst_compacted.deinit();
                for (task.upper_level_sst_ids.items) |sst_id| {
                    try l0_sst_compacted.put(sst_id, .{});
                }

                var l0 = state.l0_sstables.get();
                for (l0.ssts.items) |sst_id| {
                    if (!l0_sst_compacted.remove(sst_id)) {
                        try new_l0_sstables.append(sst_id);
                    }
                }
                std.debug.assert(l0_sst_compacted.count() == 0);

                l0.clear();
                try l0.ssts.appendSlice(new_l0_sstables.items);
            }
        }
        std.debug.assert(sliceEquals(
            task.lower_level_sst_ids,
            state.levels.items[task.lower_level - 1].get().ssts,
        ));
        try files_to_remove.appendSlice(task.lower_level_sst_ids.items);
        var lower = state.levels.items[task.lower_level - 1].get();
        lower.clear();
        try lower.ssts.appendSlice(output);

        return files_to_remove;
    }
};

fn sliceEquals(a: std.ArrayList(usize), b: std.ArrayList(usize)) bool {
    if (a.items.len != b.items.len) {
        return false;
    }
    for (a.items, 0..) |item, i| {
        if (item != b.items[i]) {
            return false;
        }
    }
    return true;
}

// ------------------ tiered compaction ------------------

pub const TieredCompactionOptions = struct {
    num_tiers: usize,
    max_size_amplification_percent: usize,
    size_ratio: usize,
    min_merge_width: usize,
    max_merge_width: ?usize,
};

pub const TieredCompactionTask = struct {
    tiers: std.ArrayList(storage.LevelPtr),
    bottom_tier_included: bool,

    pub fn deinit(self: *TieredCompactionTask) void {
        self.tiers.deinit();
    }
};

pub const TieredCompactionController = struct {
    options: TieredCompactionOptions,

    pub fn init(options: TieredCompactionOptions) TieredCompactionController {
        return .{ .options = options };
    }

    fn generateCompactionTask(self: TieredCompactionController, state: *storage.StorageState) !?TieredCompactionTask {
        std.debug.assert(state.l0_sstables.get().size() == 0);

        if (state.levels.items.len < self.options.num_tiers) return null;

        // compaction triggered by space amplification ratio
        var size: usize = 0;
        for (0..state.levels.items.len - 1) |i| {
            size += state.levels.items[i].get().size();
        }
        // std.debug.print("size: {d}, last_size: {d}\n", .{ size, state.levels.items[0].get().size() });
        const space_amp_ration = @as(
            usize,
            @intFromFloat(@as(f64, @floatFromInt(size)) / @as(f64, @floatFromInt(state.levels.items[0].get().size())) * 100.0),
        );

        if (space_amp_ration >= self.options.max_size_amplification_percent) {
            std.debug.print("compaction triggered by space amplification ratio {} >= {d}\n", .{ space_amp_ration, self.options.max_size_amplification_percent });
            return .{
                .tiers = try state.levels.clone(),
                .bottom_tier_included = true,
            };
        }

        const size_ration_trigger = (100.0 + @as(f64, @floatFromInt(self.options.size_ratio))) / 100.0;
        // compaction triggered by size ratio
        size = 0;
        for (0..state.levels.items.len - 1) |i| {
            size += state.levels.items[i].get().size();
            const next_level_size = state.levels.items[i + 1].get().size();
            const current_size_ratio = @as(f64, @floatFromInt(next_level_size)) / @as(f64, @floatFromInt(size));
            if (current_size_ratio > size_ration_trigger and (i + 1) >= self.options.min_merge_width) {
                std.debug.print("compaction triggered by size ratio {} > {}\n", .{ current_size_ratio * 100.0, size_ration_trigger * 100 });

                var tiers = try std.ArrayList(storage.LevelPtr).initCapacity(state.allocator, i + 1);
                errdefer tiers.deinit();
                try tiers.appendSlice(state.levels.items[0 .. i + 1]);

                return .{
                    .tiers = tiers,
                    .bottom_tier_included = (i + 1) == state.levels.items.len,
                };
            }
        }

        // trying to reduce sorted runs without respecting size ratio
        const num_tiers_to_take = @min(state.levels.items.len, self.options.max_merge_width orelse std.math.maxInt(usize));
        std.debug.print("compaction triggered by reducing sorted runs\n", .{});
        var tiers = try std.ArrayList(storage.LevelPtr).initCapacity(state.allocator, num_tiers_to_take);
        errdefer tiers.deinit();
        try tiers.appendSlice(state.levels.items[0..num_tiers_to_take]);
        return .{
            .tiers = tiers,
            .bottom_tier_included = state.levels.items.len == num_tiers_to_take,
        };
    }

    fn applyCompactionResult(
        _: TieredCompactionController,
        state: *storage.StorageState,
        task: TieredCompactionTask,
        output: []usize,
    ) !std.ArrayList(usize) {
        std.debug.assert(state.l0_sstables.get().size() == 0);

        var tier_to_remove = std.AutoHashMap(usize, std.ArrayList(usize)).init(state.allocator);
        defer tier_to_remove.deinit();
        for (task.tiers.items) |t| {
            const lv = t.get();
            try tier_to_remove.put(lv.tiered_id, lv.ssts);
        }

        var levels = std.ArrayList(storage.LevelPtr).init(state.allocator);
        defer levels.deinit();
        var new_tier_added = false;
        var files_to_remove = std.ArrayList(usize).init(state.allocator);
        errdefer files_to_remove.deinit();
        for (state.levels.items) |t| {
            if (tier_to_remove.fetchRemove(t.get().tiered_id)) |tier| {
                std.debug.assert(sliceEquals(tier.value, t.get().ssts));
                try files_to_remove.appendSlice(tier.value.items);
            } else {
                try levels.append(t.clone());
            }
            if (tier_to_remove.count() == 0 and !new_tier_added) {
                new_tier_added = true;
                var new_level = std.ArrayList(usize).init(state.allocator);
                errdefer new_level.deinit();
                try new_level.appendSlice(output);
                try levels.append(try storage.LevelPtr.create(state.allocator, .{
                    .tiered_id = output[0],
                    .ssts = new_level,
                }));
            }
        }
        std.debug.assert(tier_to_remove.count() == 0);
        {
            for (state.levels.items) |l| {
                var lp = l;
                lp.deinit();
            }
            state.levels.clearRetainingCapacity();
        }
        try state.levels.appendSlice(levels.items);

        return files_to_remove;
    }
};

// --------------- leveled compaction -----------------

const LeveledCompactionOptions = struct {
    level_size_multiplier: usize,
    level0_file_num_compaction_trigger: usize,
    max_levels: usize,
    base_level_size_mb: usize,
};

pub const LeveledCompactionTask = struct {
    upper_level: ?usize,
    upper_level_sst_ids: std.ArrayList(usize),
    lower_level: usize,
    lower_level_sst_ids: std.ArrayList(usize),
    is_lower_level_bottom: bool,

    pub fn deinit(self: *LeveledCompactionTask) void {
        self.upper_level_sst_ids.deinit();
        self.lower_level_sst_ids.deinit();
    }
};

const Priority = struct {
    level: usize,
    priority: f64,

    fn lessThan(context: void, a: Priority, b: Priority) bool {
        _ = context;
        return (a.priority < b.priority) or (a.priority == b.priority and a.level < b.level);
    }
};

pub const LeveledCompactionController = struct {
    options: LeveledCompactionOptions,

    pub fn init(options: LeveledCompactionOptions) LeveledCompactionController {
        return .{ .options = options };
    }

    fn generateCompactionTask(self: LeveledCompactionController, state: *storage.StorageState) !?LeveledCompactionTask {
        // step1:compute targe
        var real_level_size = try std.ArrayList(usize).initCapacity(state.allocator, self.options.max_levels);
        var base_level = self.options.max_levels;
        for (0..self.options.max_levels) |i| {
            var level_size_sum: usize = 0;
            for (state.levels.items[i].get().ssts.items) |sst_id| {
                level_size_sum += state.sstables.get(sst_id).?.get().tableSize();
            }
            try real_level_size.append(level_size_sum);
        }
        const base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        // select base level and compute target level size
        var target_level_size = try std.ArrayList(usize).initCapacity(state.allocator, self.options.max_levels);
        target_level_size.items[self.options.max_levels - 1] = @max(real_level_size.items[self.options.max_levels - 1], base_level_size_bytes);

        for (0..self.options.max_levels - 1) |i| {
            const j = self.options.max_levels - 1 - i;
            const next_level_size = target_level_size.items[j + 1];
            const this_level_size = next_level_size / self.options.level_size_multiplier;
            if (next_level_size > base_level_size_bytes) target_level_size.items[j] = this_level_size;
            if (target_level_size.items[j] > 0) base_level = j + 1;
        }

        // flush l0 sst is first priority
        if (state.l0_sstables.get().size() >= self.options.level0_file_num_compaction_trigger) {
            std.debug.print("Flushing L0 SSTables To Base Level\n", .{});
            return .{
                .upper_level = null,
                .upper_level_sst_ids = try state.l0_sstables.get().dump(),
                .lower_level = base_level,
                .lower_level_sst_ids = try self.findOverlappingSsts(state, state.l0_sstables.get().items(), base_level),
                .is_lower_level_bottom = base_level == self.options.max_levels,
            };
        }

        var priorities = try std.ArrayList(Priority).initCapacity(state.allocator, self.options.max_levels);
        defer priorities.deinit();
        for (0..self.options.max_levels) |level| {
            const p = @as(f64, @floatFromInt(real_level_size.items[level])) / @as(f64, @floatFromInt(target_level_size.items[level]));
            if (p > 1.0) try priorities.append(.{ .level = level + 1, .priority = p });
        }
        std.sort.block(Priority, priorities.items, {}, Priority.lessThan);
        const fp = priorities.getLast();
        const selected_level = state.levels.items[fp.level - 1].get();
        return .{
            .upper_level = fp.level,
            .upper_level_sst_ids = try selected_level.dump(),
            .lower_level = fp.level + 1,
            .lower_level_sst_ids = try self.findOverlappingSsts(state, selected_level.items(), fp.level + 1),
            .is_lower_level_bottom = fp.level + 1 == self.options.max_levels,
        };
    }

    fn findOverlappingSsts(
        _: LeveledCompactionController,
        state: *storage.StorageState,
        sst_ids: []const usize,
        in_level: usize,
    ) !std.ArrayList(usize) {
        var begin_key: []const u8 = undefined;
        for (sst_ids, 0..) |sst_id, i| {
            const sst = state.sstables.get(sst_id).?.get();
            if (i == 0) {
                begin_key = sst.firstKey();
            } else if (std.mem.lessThan(u8, sst.firstKey(), begin_key)) {
                begin_key = sst.firstKey();
            }
        }

        var end_key: []const u8 = undefined;
        for (sst_ids, 0..) |sst_id, i| {
            const sst = state.sstables.get(sst_id).?.get();
            if (i == 0) {
                end_key = sst.lastKey();
            } else if (std.mem.lessThan(u8, end_key, sst.lastKey())) {
                end_key = sst.lastKey();
            }
        }

        var overlapping_ssts = std.ArrayList(usize).init(state.allocator);
        for (state.levels.items[in_level - 1].get().items()) |sst_id| {
            const sst = state.sstables.get(sst_id).?.get();
            const first_key = sst.firstKey();
            const last_key = sst.lastKey();
            if (!std.mem.lessThan(u8, begin_key, last_key) and !(std.mem.lessThan(u8, end_key, first_key))) {
                try overlapping_ssts.append(sst_id);
            }
        }

        return overlapping_ssts;
    }

    fn applyCompactionResult(
        _: LeveledCompactionController,
        state: *storage.StorageState,
        task: LeveledCompactionTask,
        output: []usize,
        in_recovery: bool,
    ) !std.ArrayList(usize) {
        const allocator = state.allocator;
        var files_to_remove = std.ArrayList(usize).init(allocator);

        var upper_level_sst_ids = std.AutoHashMap(usize, struct {}).init(allocator);
        defer upper_level_sst_ids.deinit();
        for (task.lower_level_sst_ids.items) |sst_id| {
            try upper_level_sst_ids.put(sst_id, .{});
        }

        var lower_level_sst_ids = std.AutoHashMap(usize, struct {}).init(allocator);
        defer lower_level_sst_ids.deinit();
        for (task.upper_level_sst_ids.items) |sst_id| {
            try lower_level_sst_ids.put(sst_id, .{});
        }

        if (task.upper_level) |upper_level| {
            var new_upper_level_ssts = std.ArrayList(usize).init(allocator);
            defer new_upper_level_ssts.deinit();
            for (state.levels.items[upper_level - 1].get().items()) |sst_id| {
                if (!upper_level_sst_ids.remove(sst_id)) try new_upper_level_ssts.append(sst_id);
            }
            std.debug.assert(upper_level_sst_ids.count() == 0);
            state.levels.items[upper_level - 1].get().clear();
            try state.levels.items[upper_level - 1].get().appendSlice(new_upper_level_ssts.items);
        } else {
            var new_l0_ssts = std.ArrayList(usize).init(allocator);
            defer new_l0_ssts.deinit();
            for (state.levels.items[0].get().items()) |sst_id| {
                if (!upper_level_sst_ids.remove(sst_id)) try new_l0_ssts.append(sst_id);
            }
            std.debug.assert(upper_level_sst_ids.count() == 0);
            state.l0_sstables.get().clear();
            try state.l0_sstables.get().appendSlice(new_l0_ssts.items);
        }

        try files_to_remove.appendSlice(task.upper_level_sst_ids.items);
        try files_to_remove.appendSlice(task.lower_level_sst_ids.items);

        var new_lower_level_ssts = std.ArrayList(usize).init(allocator);
        defer new_lower_level_ssts.deinit();
        for (state.levels.items[1].get().items()) |sst_id| {
            if (!lower_level_sst_ids.remove(sst_id)) try new_lower_level_ssts.append(sst_id);
        }
        std.debug.assert(lower_level_sst_ids.count() == 0);
        try new_lower_level_ssts.appendSlice(output);

        // Don't sort the SST IDs during recovery because actual SSTs are not loaded at that point
        if (!in_recovery) {
            const Ctx = struct {
                sstables: std.AutoHashMap(usize, sstable.SsTablePtr),
            };
            const Comparer = struct {
                fn lessThan(context: Ctx, lhs: usize, rhs: usize) bool {
                    const x = context.sstables.get(lhs).?.get().firstKey();
                    const y = context.sstables.get(rhs).?.get().firstKey();
                    return std.mem.lessThan(u8, x, y);
                }
            };
            std.sort.block(
                usize,
                new_lower_level_ssts.items,
                Ctx{ .sstables = state.sstables },
                Comparer.lessThan,
            );
        }
        state.levels.items[task.lower_level - 1].get().clear();
        try state.levels.items[task.lower_level - 1].get().appendSlice(new_lower_level_ssts.items);
        return files_to_remove;
    }
};
