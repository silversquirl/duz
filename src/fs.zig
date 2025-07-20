//! Filesystem utilities

pub fn fileSize(dirfd: linux.fd_t, path: [*:0]const u8) !u64 {
    const tr = tracy.trace(@src());
    defer tr.end();

    var stx: linux.Statx = undefined;
    const rc = linux.statx(
        dirfd,
        path,
        linux.AT.EMPTY_PATH | linux.AT.SYMLINK_NOFOLLOW | linux.AT.STATX_DONT_SYNC,
        linux.STATX_SIZE,
        &stx,
    );
    switch (linux.E.init(rc)) {
        .SUCCESS => {},
        .ACCES => return error.AccessDenied,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => unreachable,
        .LOOP => unreachable,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOTDIR => return error.NotDir,
        else => |err| return std.posix.unexpectedErrno(err),
    }
    return stx.size;
}

pub fn listDir(gpa: std.mem.Allocator, string_arena: std.mem.Allocator, fd: linux.fd_t) !Listing {
    const tr = tracy.trace(@src());
    defer tr.end();

    var entries: std.ArrayListUnmanaged(Listing.Entry) = .empty;
    errdefer entries.deinit(gpa);

    // Modest starting capacity
    try entries.ensureTotalCapacity(gpa, 32);

    // OPTIM: it may be faster to just read directly into the arraylist
    // OPTIM: determine optimal buffer size
    var dents_buf: [4 << 10]u8 = undefined;
    var dir_count: usize = 0;
    var file_count: usize = 0;
    while (true) {
        const size = blk: {
            const tr_ = tracy.traceNamed(@src(), "getdents64");
            defer tr_.end();

            const rc = std.os.linux.getdents64(fd, &dents_buf, dents_buf.len);
            switch (std.os.linux.E.init(rc)) {
                .SUCCESS => {},

                .BADF => unreachable, // Dir is invalid or was opened without iteration ability
                .FAULT => unreachable,
                .NOTDIR => unreachable,

                .NOENT => return error.DirNotFound, // The directory being iterated was deleted during iteration.
                .INVAL => return error.Unexpected, // Linux may in some cases return EINVAL when reading /proc/$PID/net.
                .ACCES => return error.AccessDenied, // Do not have permission to iterate this directory.
                else => |err| return std.posix.unexpectedErrno(err),
            }

            break :blk rc;
        };

        if (size == 0) break;

        var off: usize = 0;
        while (off < size) {
            const tr_ = tracy_verbose.traceNamed(@src(), "listDir iteration");
            defer tr_.end();
            tr_.setColor(0x888888);

            const dent: *align(1) linux.dirent64 = @ptrCast(dents_buf[off..]);
            off += dent.reclen;

            const name = std.mem.span(@as([*:0]u8, @ptrCast(&dent.name)));
            tr_.setName(name);

            if (isDotOrDotDot(name)) continue;

            const kind: Listing.Entry.Kind = switch (dent.type) {
                linux.DT.BLK => .block_device,
                linux.DT.CHR => .character_device,
                linux.DT.DIR => .directory,
                linux.DT.FIFO => .named_pipe,
                linux.DT.LNK => .sym_link,
                linux.DT.REG => .file,
                linux.DT.SOCK => .unix_domain_socket,
                else => .unknown,
            };

            if (kind == .directory) {
                dir_count += 1;
            } else {
                file_count += 1;
            }

            comptime std.debug.assert(@sizeOf(@TypeOf(kind)) == 1);
            try entries.append(gpa, .{
                .name = try string_arena.dupeZ(u8, name),
                .kind = kind,
            });
        }
    }

    return .{
        .entries = try entries.toOwnedSlice(gpa),
        .dir_count = dir_count,
    };
}

/// Returns true if the string is equal to "." or ".."
fn isDotOrDotDot(name: [:0]const u8) bool {
    // Doing this manually optimizes better than chaining std.mem.eql
    if (name.len == 0) return false;
    if (name[0] != '.') return false;
    return name[1] == '.' or name[1] == 0; // exploit sentinel to avoid an extra branch
}

pub const Listing = struct {
    entries: []Entry,
    dir_count: usize,

    /// does not free paths; they're in the worker's arena
    pub fn deinit(l: Listing, gpa: std.mem.Allocator) void {
        gpa.free(l.entries);
    }

    pub const Entry = struct {
        name: [*:0]const u8,
        kind: Kind,
        pub const Kind = std.fs.Dir.Entry.Kind;
    };
};

const std = @import("std");
const linux = std.os.linux;

const tracy = @import("tracy");
const trace_level = @import("build_options").trace_level;
const tracy_verbose = if (trace_level == .verbose) tracy else @import("tracy_always_disabled");
