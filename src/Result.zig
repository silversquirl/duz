const Result = @This();

parent: u32,
path: [*:0]const u8,
size: u64 = 0,
state: State.Packed,

pub const State = union(enum) {
    incomplete_directory: u31, // Payload is the number of incomplete children
    completed_directory: void,

    incomplete_file: void,
    completed_file: void,

    errored: anyerror, // TODO: more specific error set

    pub const uninitialized_directory: State = .{
        // This is fine because it only really matters whether the count is zero or not.
        .incomplete_directory = std.math.maxInt(u31),
    };

    pub const Packed = packed union {
        directory: packed struct(u32) {
            incomplete_children: u31,
            not_a_directory: bool = false,
        },
        file: packed struct(u32) {
            state: enum(u30) {
                incomplete,
                completed,
            },
            tag: Tag = .file,
        },
        errored: packed struct(u32) {
            err_int: u30,
            tag: Tag = .errored,
        },

        pub const Tag = enum(u2) {
            directory0 = 0b00,
            directory1 = 0b01,
            errored = 0b10,
            file = 0b11,
        };

        pub fn pack(state: State) Packed {
            return switch (state) {
                .incomplete_directory => |c| .{ .directory = .{ .incomplete_children = c } },
                .completed_directory => .{ .directory = .{ .incomplete_children = 0 } },

                .incomplete_file => .{ .file = .{ .state = .incomplete } },
                .completed_file => .{ .file = .{ .state = .completed } },

                .errored => .{ .errored = .{ .err_int = @intFromError(state.errored) } },
            };
        }

        pub fn unpack(state: Packed) State {
            return switch (state.file.tag) {
                .file => switch (state.file.state) {
                    .incomplete => .incomplete_file,
                    .completed => .completed_file,
                },
                .errored => blk: {
                    const ErrorInt = std.meta.Int(.unsigned, @bitSizeOf(anyerror));
                    const casted: ErrorInt = @intCast(state.errored.err_int);
                    break :blk .{ .errored = @errorFromInt(casted) };
                },
                .directory0, .directory1 => if (state.directory.incomplete_children == 0)
                    .completed_directory
                else
                    .{ .incomplete_directory = state.directory.incomplete_children },
            };
        }

        /// Decrease the number of incomplete children in an `incomplete_directory` state.
        /// Asserts that the state contains an `incomplete_directory` value.
        pub fn finishChildren(state: *Packed, count: u31) void {
            std.debug.assert(!state.directory.not_a_directory);
            state.directory.incomplete_children -= count;
        }

        comptime {
            const one_left: Packed = .pack(.{ .incomplete_directory = 1 });
            const completed: Packed = .pack(.completed_directory);

            std.debug.assert(@as(u32, @bitCast(one_left)) == 1);
            std.debug.assert(@as(u32, @bitCast(completed)) == 0);
        }
    };

    pub const Atomic = extern struct {
        // For some reason, Zig doesn't allow atomics on packed unions
        value: std.atomic.Value(std.meta.Int(.unsigned, @bitSizeOf(Packed))),

        pub fn init(state: State) Atomic {
            return .{ .value = .init(@bitCast(Packed.pack(state))) };
        }

        pub fn load(state: *Atomic, comptime order: std.builtin.AtomicOrder) Packed {
            return @bitCast(state.value.load(order));
        }
        pub fn store(state: *Atomic, value: Packed, comptime order: std.builtin.AtomicOrder) void {
            state.value.store(@bitCast(value), order);
        }

        /// Atomically decrease the number of incomplete children in an `incomplete_directory` state. Returns the new state.
        /// Asserts that the state contains an `incomplete_directory` value.
        pub fn finishChildren(state: *Atomic, count: u31, comptime order: std.builtin.AtomicOrder) Packed {
            const prev_int = state.value.fetchSub(@bitCast(@as(u32, count)), order);
            const prev: Packed = @bitCast(prev_int);
            std.debug.assert(!prev.directory.not_a_directory);
            return @bitCast(prev_int - count);
        }
    };
};

pub const ThreadSafe = struct {
    parent: u32,
    path: [*:0]const u8,
    size: std.atomic.Value(u64) = .init(0),
    state: State.Atomic,

    /// This function is not fully thread safe, and should not be relied upon to produce coherent results without external synchronization.
    pub fn load(result: *ThreadSafe) Result {
        return .{
            .parent = result.parent,
            .path = result.path,
            .size = result.size.load(.monotonic),
            .state = result.state.load(.monotonic),
        };
    }
};

const std = @import("std");
