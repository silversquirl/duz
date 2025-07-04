pub fn Store(comptime Index: type, comptime Value: type) type {
    return struct {
        items: std.SegmentedList(Value, 32),
        free: ?Index,

        comptime {
            if (@sizeOf(Index) > @sizeOf(Value)) {
                @compileError("Index type must be smaller than Value type");
            }
        }

        const Self = @This();

        pub const empty: Self = .{
            .items = .{},
            .free = null,
        };

        pub fn deinit(store: *Self, gpa: std.mem.Allocator) void {
            store.items.deinit(gpa);
        }

        pub fn get(store: *Self, index: Index) *Value {
            const i = switch (@typeInfo(Index)) {
                .@"enum" => @intFromEnum(index),
                .int, .comptime_int => index,
                else => @compileError("index must be an enum or integer, got " ++ @typeName(@TypeOf(index))),
            };
            return store.items.at(i);
        }

        /// Allocate and initialize an entry
        pub fn put(store: *Self, gpa: std.mem.Allocator, value: Value) !Index {
            const index = try store.add(gpa);
            store.get(index).* = value;
            return index;
        }

        /// Allocate but do not initialize an entry
        pub fn add(store: *Self, gpa: std.mem.Allocator) !Index {
            if (store.free) |index| {
                const ptr = store.get(index);
                const free: *align(@alignOf(Value)) Index = @ptrCast(ptr);
                if (free.* == index) {
                    store.free = null;
                } else {
                    store.free = free.*;
                }
                ptr.* = undefined;
                return index;
            } else {
                const index = store.items.len;
                try store.items.append(gpa, undefined);
                return switch (@typeInfo(Index)) {
                    .@"enum" => @enumFromInt(index),
                    .int, .comptime_int => @intCast(index),
                    else => @compileError("index must be an enum or integer, got " ++ @typeName(Index)),
                };
            }
        }

        pub fn del(store: *Self, index: Index) void {
            const ptr = store.get(index);
            const free: *align(@alignOf(Value)) Index = @ptrCast(ptr);
            free.* = store.free orelse index;
            store.free = index;
        }
    };
}

const std = @import("std");
