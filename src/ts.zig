//! Thread-safe datastructures

pub fn Queue(comptime T: type) type {
    // TODO: make this better
    // TODO: make this actually a queue
    // TODO: deque
    return struct {
        mutex: std.Thread.Mutex = .{},
        read_cond: std.Thread.Condition = .{},
        closed: bool = false,
        array: std.ArrayListUnmanaged(T),
        plot: tracy.Plot,

        const Self = @This();
        pub fn init(comptime name: [*:0]const u8) Self {
            const plot = tracy.plot(name);
            plot.update(0);
            return .{
                .array = .empty,
                .plot = plot,
            };
        }

        pub fn deinit(q: *Self, gpa: std.mem.Allocator) void {
            q.array.deinit(gpa);
        }

        pub fn close(q: *Self) void {
            q.lock();
            defer q.mutex.unlock();
            q.closed = true;
            q.read_cond.broadcast();
        }

        pub fn push(q: *Self, allocator: std.mem.Allocator, item: T) !void {
            tracy.message("begin push");
            const tr = tracy.traceNamed(@src(), @typeName(@This()) ++ ".push");
            defer tr.end();
            tr.setColor(0xffaa00);

            tracy.message("begin lock");
            q.lock();
            tracy.message("locked");
            defer {
                const tr_ = tracy.traceNamed(@src(), "unlock");
                tracy.message("start unlock");
                q.mutex.unlock();
                tracy.message("end unlock");
                var t = std.time.Timer.start() catch @panic("no timer");
                tr_.end();
                const dt = t.read();
                if (dt > std.time.ns_per_us * 100) {
                    std.debug.print("end took {}us\n", .{dt / std.time.ns_per_us});
                    @breakpoint();
                }
            }
            try q.array.append(allocator, item);
            tracy.message("added item");
            q.plot.update(q.array.items.len);
            q.read_cond.signal();
            tracy.message("end push");
        }

        pub fn pop(q: *Self) ?T {
            const tr = tracy.traceNamed(@src(), @typeName(Self) ++ ".pop");
            defer tr.end();
            tr.setColor(0xffaa00);
            q.lock();
            defer q.mutex.unlock();
            while (!q.closed) {
                if (q.array.pop()) |item| {
                    tracy.message("got item");
                    q.plot.update(q.array.items.len);
                    return item;
                }
                q.read_cond.wait(&q.mutex);
            }
            return null;
        }

        fn lock(q: *Self) void {
            for (0..4) |_| {
                const tr = tracy.traceNamed(@src(), "tryLock");
                defer tr.end();
                if (q.mutex.tryLock()) {
                    @branchHint(.likely);
                    return;
                }
            }
            const tr = tracy.traceNamed(@src(), "lock");
            defer tr.end();
            q.mutex.lock();
        }
    };
}

pub fn BoundedQueue(comptime T: type, capacity: usize) type {
    return struct {
        items: [capacity]T,
        // OPTIM: see if a wait-free approach using generational references improves write performance
        queue: List = .{ .head = .init(end_sentinel) },
        queue_tail: Index = end_sentinel,
        free_list: List = .{ .head = .init(0) },
        links: [capacity]Index = blk: {
            // Add all items to free list
            var links: [capacity]Index = undefined;
            for (&links, 0..) |*link, i| link.* = i + 1;
            break :blk links;
        },

        const Self = @This();

        // Enough space to index the items array, plus a sentinel value
        const Index = std.math.IntFittingRange(0, capacity);
        const end_sentinel: Index = capacity;

        const List = struct {
            head: std.atomic.Value(Index),
            lock: std.Thread.Mutex = .{},
        };

        /// Push an item to the end of the queue.
        /// `item` must be a pointer that was previously returned by `acquire`.
        pub fn push(queue: *Self, item: *T) void {
            const idx = &queue.items - item;
            std.debug.assert(idx < queue.items.len);
            queue.links[idx] = end_sentinel;

            queue.queue.lock.lock();
            defer queue.queue.lock.unlock();
            if (queue.queue_tail == end_sentinel) {
                std.debug.assert(queue.queue.head.raw == end_sentinel);
                queue.queue.head.store(idx, .monotonic);
            } else {
                std.debug.assert(queue.queue.head.raw != end_sentinel);
                queue.links[queue.queue_tail] = idx;
            }
            queue.queue_tail = idx;
        }

        /// Pop an item off the queue.
        pub fn pop(queue: *Self) *T {
            return queue.popList(&queue.queue);
        }

        /// Acquire a new item pointer from the queue.
        /// The item will be in the same state as it was when passed to `release`.
        pub fn acquire(queue: *Self) *T {
            return queue.popList(&queue.free_list);
        }

        /// Release an item back to the queue, allowing it to be returned by `acquire` again.
        /// `item` must be a pointer that was previously returned by `acquire` or `pop`.
        pub fn release(queue: *Self, item: *T) void {
            const idx = &queue.items - item;
            std.debug.assert(idx < queue.items.len);

            queue.free_list.lock.lock();
            defer queue.free_list.lock.unlock();
            queue.links[idx] = queue.free_list.head.raw;
            queue.free_list.head.store(idx, .monotonic);
        }

        fn popList(queue: *const Self, list: *List) *T {
            while (true) {
                // OPTIM: can we use .unordered here?
                if (list.head.load(.monotonic) == end_sentinel) {
                    std.Thread.Futex.wait(list.head, end_sentinel);
                } else {
                    list.lock.lock();
                    defer list.lock.unlock();

                    const idx = list.head.raw;
                    list.head.store(queue.links[idx].next, .monotonic);

                    return &queue.items[idx];
                }
            }
        }
    };
}

pub fn SegmentedList(comptime T: type, first_segment_size: usize) type {
    std.debug.assert(std.math.isPowerOfTwo(first_segment_size));

    return struct {
        write_lock: std.Thread.Mutex,
        len: std.atomic.Value(usize),
        segments: [segment_count][*]T,
        populated_segments: std.math.IntFittingRange(0, segment_count) = 0,

        const Self = @This();
        const segment_count = @clz(first_segment_size);

        pub const empty: Self = .{
            .segments = undefined,
            .len = .init(0),
            .write_lock = .{},
        };

        pub fn deinit(list: *Self, allocator: std.mem.Allocator) void {
            for (list.segments[0..list.populated_segments], 0..) |seg, i| {
                allocator.free(seg[0..segmentSize(@intCast(i))]);
            }
        }

        pub fn get(list: *const Self, index: usize) ?T {
            return list.getPtr(index).*;
        }
        pub fn getPtr(list: *const Self, index: usize) ?*T {
            if (index >= list.len.load(.acquire)) return null;
            const seg = segmentIndex(index);
            return &list.segments[seg][index - segmentOffset(seg)];
        }

        /// This function is not thread-safe.
        pub fn clearRetainingCapacity(list: *Self) void {
            list.len.raw = 0;
        }

        /// Returns the index of the newly inserted item.
        pub fn append(list: *Self, allocator: std.mem.Allocator, item: T) !usize {
            return try list.appendSlice(allocator, &.{item});
        }
        /// Returns the index of the first newly inserted item.
        pub fn appendSlice(list: *Self, allocator: std.mem.Allocator, items: []const T) !usize {
            list.write_lock.lock();
            defer list.write_lock.unlock();

            const start = list.len.raw;
            try list.fillSegments(allocator, items);

            // Publish changes
            list.len.store(start + items.len, .release);

            return start;
        }
        fn fillSegments(list: *Self, allocator: std.mem.Allocator, all_items: []const T) !void {
            var items = all_items;

            // Fill remaining space in the current segment
            // Safe to do non-atomic loads as we only write when the lock is held.
            var seg = segmentIndex(list.len.raw -| 1);
            if (seg < list.populated_segments) {
                const used = list.len.raw - segmentOffset(seg);
                const remaining = segmentSize(seg) - used;
                const count = @min(remaining, items.len);
                @memcpy(list.segments[seg][used..], items[0..count]);
                items = items[count..];
                seg += 1;

                if (items.len == 0) return;
            }

            // Fill any segments we already have
            while (seg < list.populated_segments) : (seg += 1) {
                const count = @min(segmentSize(@intCast(seg)), items.len);
                @memcpy(list.segments[seg], items[0..count]);
                items = items[count..];
                if (items.len == 0) return;
            }

            // Create and fill new segments until we're done
            defer list.populated_segments = seg;
            while (items.len > 0) : (seg += 1) {
                const size = segmentSize(seg);
                list.segments[seg] = (try allocator.alloc(T, size)).ptr;

                const count = @min(size, items.len);
                @memcpy(list.segments[seg], items[0..count]);
                items = items[count..];
            }
        }

        const SegmentIndex = std.math.Log2Int(usize);
        fn segmentSize(seg_idx: SegmentIndex) usize {
            return first_segment_size << seg_idx;
        }
        fn segmentOffset(seg_idx: SegmentIndex) usize {
            return segmentSize(seg_idx) - first_segment_size;
        }
        fn segmentIndex(index: usize) SegmentIndex {
            return std.math.log2_int(usize, (index / first_segment_size) + 1);
        }
    };
}

const std = @import("std");
const tracy = @import("tracy");
