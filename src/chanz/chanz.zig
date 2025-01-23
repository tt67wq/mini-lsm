const std = @import("std");
const fifo = std.fifo;

const ChanError = error{
    Closed,
    OutOfMemory,
    NotImplemented,
    DataCorruption,
};

// represents a thread waiting on recv Receiver = struct {
fn Receiver(comptime T: type) type {
    return struct {
        mut: std.Thread.Mutex = std.Thread.Mutex{},
        cond: std.Thread.Condition = std.Thread.Condition{},
        data: ?T = null,

        fn putDataAndSignal(self: *@This(), data: T) void {
            defer self.cond.signal();
            self.data = data;
        }
    };
}

// represents a thread waiting on send
fn Sender(comptime T: type) type {
    return struct {
        mut: std.Thread.Mutex = std.Thread.Mutex{},
        cond: std.Thread.Condition = std.Thread.Condition{},
        data: T,
        fn getDataAndSignal(self: *@This()) T {
            defer self.cond.signal();
            return self.data;
        }
    };
}

fn Chan(comptime T: type) type {
    return struct {
        const Self = @This();
        const rType = Receiver(T);
        const sType = Sender(T);

        mut: std.Thread.Mutex = std.Thread.Mutex{},
        alloc: std.mem.Allocator = undefined,
        recvQ: std.ArrayList(*rType) = undefined,
        sendQ: std.ArrayList(*sType) = undefined,
        closed: bool = false,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .alloc = alloc,
                .recvQ = std.ArrayList(*rType).init(alloc),
                .sendQ = std.ArrayList(*sType).init(alloc),
            };
        }

        pub fn deinit(self: *Self) void {
            self.recvQ.deinit();
            self.sendQ.deinit();
        }

        pub fn close(self: *Self) void {
            self.closed = true;
        }

        pub fn capacity(_: *Self) u8 {
            return 0;
        }

        pub fn debugBuf(_: *Self) !void {
            std.debug.print("unbuffered chan has no buf\n");
        }

        pub fn send(self: *Self, data: T) ChanError!void {
            if (self.closed) return ChanError.Closed;

            self.mut.lock();
            errdefer self.mut.unlock();

            // case: receiver already waiting
            // pull receiver (if any) and give it data. Signal receiver that it's done waiting.
            if (self.recvQ.items.len > 0) {
                defer self.mut.unlock();
                var receiver: *rType = self.recvQ.orderedRemove(0);
                receiver.putDataAndSignal(data);
                return;
            }
            // hold on sender queue. Receivers will signal when they take data.
            var sender = sType{ .data = data };

            // prime condition
            sender.mut.lock(); // cond.wait below will unlock it and wait until signal, then relock it
            defer sender.mut.unlock(); // unlocks the relock

            try self.sendQ.append(&sender); // make visible to other threads
            self.mut.unlock(); // allow all other threads to proceed. This thread is done reading/writing

            // now just wait for receiver to signal sender
            sender.cond.wait(&sender.mut);
            return;
        }

        pub fn justSend(self: *Self, data: T) ChanError!void {
            if (self.closed) return ChanError.Closed;
            self.mut.lock();
            errdefer self.mut.unlock();

            // case: receiver already waiting
            // pull receiver (if any) and give it data. Signal receiver that it's done waiting.
            if (self.recvQ.items.len > 0) {
                defer self.mut.unlock();
                var receiver: *rType = self.recvQ.orderedRemove(0);
                receiver.putDataAndSignal(data);
                return;
            }
            // no receiver waiting. Just return
            return;
        }

        pub fn recv(self: *Self) ChanError!T {
            if (self.closed) return ChanError.Closed;
            self.mut.lock();
            errdefer self.mut.unlock();

            // case: sender already waiting
            // pull sender and take its data. Signal sender that it's done waiting.
            if (self.sendQ.items.len > 0) {
                defer self.mut.unlock();
                var sender: *sType = self.sendQ.orderedRemove(0);
                const data: T = sender.getDataAndSignal();
                return data;
            }

            // hold on receiver queue. Senders will signal when they take it.
            var receiver = rType{};

            // prime condition
            receiver.mut.lock();
            defer receiver.mut.unlock();

            try self.recvQ.append(&receiver);
            self.mut.unlock();

            // now wait for sender to signal receiver
            receiver.cond.wait(&receiver.mut);
            // sender should have put data in .data
            if (receiver.data) |data| {
                return data;
            } else {
                return ChanError.DataCorruption;
            }
        }

        pub fn justRecv(self: *Self) ChanError!?T {
            if (self.closed) return ChanError.Closed;
            self.mut.lock();
            errdefer self.mut.unlock();

            // case: sender already waiting
            // pull sender and take its data. Signal sender that it's done waiting.
            if (self.sendQ.items.len > 0) {
                defer self.mut.unlock();
                var sender: *sType = self.sendQ.orderedRemove(0);
                const data: T = sender.getDataAndSignal();
                return data;
            }

            // no sender waiting. Just return null
            return null;
        }
    };
}

fn BufferedChan(comptime T: type, comptime bufSize: u8) type {
    return struct {
        const Self = @This();
        const qtype = fifo.LinearFifo(T, fifo.LinearFifoBufferType{ .Static = bufSize });
        const rType = Receiver(T);
        const sType = Sender(T);

        q: qtype = undefined,
        _capacity: u8 = bufSize,
        closed: bool = false,
        mut: std.Thread.Mutex = std.Thread.Mutex{},
        alloc: std.mem.Allocator = undefined,
        recvQ: std.ArrayList(*rType) = undefined,
        sendQ: std.ArrayList(*sType) = undefined,

        pub fn init(alloc: std.mem.Allocator) Self {
            return Self{
                .q = qtype.init(),
                .alloc = alloc,
                .recvQ = std.ArrayList(*rType).init(alloc),
                .sendQ = std.ArrayList(*sType).init(alloc),
            };
        }

        pub fn deinit(self: *Self) void {
            self.q.deinit();
            self.recvQ.deinit();
            self.sendQ.deinit();
        }

        pub fn close(self: *Self) void {
            self.closed = true;
        }

        pub fn debugBuf(self: *Self) !void {
            std.debug.print("{d} Buffer debug\n", .{std.time.milliTimestamp()});
            const buf = try self.q.toOwnedSlice();
            for (buf, 0..) |item, i| {
                if (item) |unwrapped| {
                    std.debug.print("[{d}] = {d}\n", .{ i, unwrapped });
                }
            }
        }

        pub fn capacity(self: *Self) u8 {
            return self._capacity;
        }

        fn len(self: *Self) u8 {
            return @intCast(self.q.readableLength());
        }

        fn hasRoom(self: *Self) bool {
            return self.len() < self._capacity;
        }

        pub fn send(self: *Self, data: T) ChanError!void {
            if (self.closed) return ChanError.Closed;

            self.mut.lock();
            errdefer self.mut.unlock();

            // case: receiver already waiting
            // pull receiver (if any) and give it data. Signal receiver that it's done waiting.
            if (self.recvQ.items.len > 0) {
                defer self.mut.unlock();
                var receiver: *rType = self.recvQ.orderedRemove(0);
                receiver.putDataAndSignal(data);
                return;
            }

            if (self._capacity > 0 and self.hasRoom()) {
                defer self.mut.unlock();
                self.q.writeItemAssumeCapacity(data);
                return;
            }

            // hold on sender queue. Receivers will signal when they take data.
            var sender = sType{ .data = data };

            // prime condition
            sender.mut.lock(); // cond.wait below will unlock it and wait until signal, then relock it
            defer sender.mut.unlock(); // unlocks the relock

            try self.sendQ.append(&sender); // make visible to other threads
            self.mut.unlock(); // allow all other threads to proceed. This thread is done reading/writing

            // now just wait for receiver to signal sender
            sender.cond.wait(&sender.mut);
            return;
        }

        pub fn justSend(self: *Self, data: T) ChanError!void {
            if (self.closed) return ChanError.Closed;
            self.mut.lock();
            errdefer self.mut.unlock();

            // case: receiver already waiting
            // pull receiver (if any) and give it data. Signal receiver that it's done waiting.
            if (self.recvQ.items.len > 0) {
                defer self.mut.unlock();
                var receiver: *rType = self.recvQ.orderedRemove(0);
                receiver.putDataAndSignal(data);
                return;
            }

            // no receiver waiting, put in buffer
            if (self._capacity > 0 and self.hasRoom()) {
                defer self.mut.unlock();
                self.q.writeItemAssumeCapacity(data);
                return;
            }

            // no buffer room. Just return
            return;
        }

        pub fn recv(self: *Self) ChanError!T {
            if (self.closed) return ChanError.Closed;
            self.mut.lock();
            errdefer self.mut.unlock();

            // case: value in buffer
            if (self._capacity > 0 and self.len() > 0) {
                defer self.mut.unlock();
                const val = self.q.readItem().?;

                // top up buffer with a waiting sender, if any
                if (self.sendQ.items.len > 0) {
                    var sender: *sType = self.sendQ.orderedRemove(0);
                    const valFromSender: T = sender.getDataAndSignal();
                    self.q.writeItemAssumeCapacity(valFromSender);
                }
                return val;
            }

            // case: sender already waiting
            // pull sender and take its data. Signal sender that it's done waiting.
            if (self.sendQ.items.len > 0) {
                defer self.mut.unlock();
                var sender: *sType = self.sendQ.orderedRemove(0);
                const data: T = sender.getDataAndSignal();
                return data;
            }

            // hold on receiver queue. Senders will signal when they take it.
            var receiver = rType{};

            // prime condition
            receiver.mut.lock();
            defer receiver.mut.unlock();

            try self.recvQ.append(&receiver);
            self.mut.unlock();

            // now wait for sender to signal receiver
            receiver.cond.wait(&receiver.mut);
            // sender should have put data in .data
            if (receiver.data) |data| {
                return data;
            } else {
                return ChanError.DataCorruption;
            }
        }

        pub fn justRecv(self: *Self) ChanError!?T {
            if (self.closed) return ChanError.Closed;
            self.mut.lock();
            errdefer self.mut.unlock();

            // case: value in buffer
            if (self._capacity > 0 and self.len() > 0) {
                defer self.mut.unlock();
                const val = self.q.readItem().?;

                // top up buffer with a waiting sender, if any
                if (self.sendQ.items.len > 0) {
                    var sender: *sType = self.sendQ.orderedRemove(0);
                    const valFromSender: T = sender.getDataAndSignal();
                    self.q.writeItemAssumeCapacity(valFromSender);
                }
                return val;
            }

            // case: no value in buffer, but sender waiting
            if (self.sendQ.items.len > 0) {
                defer self.mut.unlock();
                var sender: *sType = self.sendQ.orderedRemove(0);
                const data: T = sender.getDataAndSignal();
                return data;
            }

            // no value in buffer, no sender waiting. Just return null
            return null;
        }
    };
}

test "unbufferedChan" {
    // create channel of u8
    const T = Chan(u8);
    var chan = T.init(std.testing.allocator);
    defer chan.deinit();

    // spawn thread that immediately waits on channel
    const thread = struct {
        fn func(c: *T) !void {
            const val = try c.recv();
            std.debug.print("{d} Thread Received {d}\n", .{ std.time.milliTimestamp(), val });
        }
    };
    const t = try std.Thread.spawn(.{}, thread.func, .{&chan});
    defer t.join();

    // let thread wait a bit before sending value
    std.time.sleep(1_000_000_000);

    const val: u8 = 10;
    std.debug.print("{d} Main Sending {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);
}

test "bidirectional unbufferedChan" {
    std.debug.print("\n", .{});

    const T = Chan(u8);
    var chan = T.init(std.testing.allocator);
    defer chan.deinit();

    const thread = struct {
        fn func(c: *T) !void {
            std.time.sleep(2_000_000_000);
            const val = try c.recv();
            std.debug.print("{d} Thread Received {d}\n", .{ std.time.milliTimestamp(), val });
            std.time.sleep(1_000_000_000);
            std.debug.print("{d} Thread Sending {d}\n", .{ std.time.milliTimestamp(), val + 1 });
            try c.send(val + 1);
            std.time.sleep(2_000_000_000);
            std.debug.print("{d} Thread Sending {d}\n", .{ std.time.milliTimestamp(), val + 100 });
            try c.send(val + 100);
            std.debug.print("{d} Thread Exit\n", .{std.time.milliTimestamp()});
        }
    };

    const t = try std.Thread.spawn(.{}, thread.func, .{&chan});
    defer t.join();

    std.time.sleep(1_000_000_000);
    var val: u8 = 10;
    std.debug.print("{d} Main Sending {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);
    val = try chan.recv();
    std.debug.print("{d} Main Received {d}\n", .{ std.time.milliTimestamp(), val });
    val = try chan.recv();
    std.debug.print("{d} Main Received {d}\n", .{ std.time.milliTimestamp(), val });
}

test "buffered Chan" {
    std.debug.print("\n", .{});

    const T = BufferedChan(u8, 3);
    var chan = T.init(std.testing.allocator);
    defer chan.deinit();

    const thread = struct {
        fn func(c: *T) !void {
            std.time.sleep(2_000_000_000);
            std.debug.print("{d} Thread Receiving\n", .{std.time.milliTimestamp()});
            var val = try c.recv();
            std.debug.print("{d} Thread Received {d}\n", .{ std.time.milliTimestamp(), val });
            std.time.sleep(1_000_000_000);
            std.debug.print("{d} Thread Receiving\n", .{std.time.milliTimestamp()});
            val = try c.recv();
            std.debug.print("{d} Thread Received {d}\n", .{ std.time.milliTimestamp(), val });
            std.time.sleep(1_000_000_000);
            std.debug.print("{d} Thread Receiving\n", .{std.time.milliTimestamp()});
            val = try c.recv();
            std.debug.print("{d} Thread Received {d}\n", .{ std.time.milliTimestamp(), val });
            std.time.sleep(1_000_000_000);
            std.debug.print("{d} Thread Receiving\n", .{std.time.milliTimestamp()});
            val = try c.recv();
            std.debug.print("{d} Thread Received {d}\n", .{ std.time.milliTimestamp(), val });
        }
    };

    const t = try std.Thread.spawn(.{}, thread.func, .{&chan});
    defer t.join();

    std.time.sleep(1_000_000_000);
    var val: u8 = 10;
    std.debug.print("{d} Main Sending {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);
    std.debug.print("{d} Main Sent {d}\n", .{ std.time.milliTimestamp(), val });

    val = 11;
    std.debug.print("{d} Main Sending {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);
    std.debug.print("{d} Main Sent {d}\n", .{ std.time.milliTimestamp(), val });

    val = 12;
    std.debug.print("{d} Main Sending {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);
    std.debug.print("{d} Main Sent {d}\n", .{ std.time.milliTimestamp(), val });

    val = 13;
    std.debug.print("{d} Main Sending {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);
    std.debug.print("{d} Main Sent {d}\n", .{ std.time.milliTimestamp(), val });
}

test "chan of chan" {
    std.debug.print("\n", .{});

    const T = BufferedChan(u8, 3);
    const TofT = Chan(T);
    var chanOfChan = TofT.init(std.testing.allocator);
    defer chanOfChan.deinit();

    const thread = struct {
        fn func(cOC: *TofT) !void {
            std.time.sleep(2_000_000_000);
            std.debug.print("{d} Thread Receiving\n", .{std.time.milliTimestamp()});
            var c = try cOC.recv();
            std.debug.print("{d} Thread Received chan of chan: {any}\n", .{ std.time.milliTimestamp(), cOC });
            std.debug.print("{d} Thread pulling from chan buffer\n", .{std.time.milliTimestamp()});
            const val = try c.recv(); // should have value on buffer
            std.debug.print("{d} Thread received from chan: {d}\n", .{ std.time.milliTimestamp(), val });
        }
    };

    const t = try std.Thread.spawn(.{}, thread.func, .{&chanOfChan});
    defer t.join();

    std.time.sleep(1_000_000_000);
    const val: u8 = 10;
    var chan = T.init(std.testing.allocator);
    defer chan.deinit();
    std.debug.print("{d} Main sending u8 to chan {d}\n", .{ std.time.milliTimestamp(), val });
    try chan.send(val);

    std.debug.print("{d} Main sending chan across chanOfChan\n", .{std.time.milliTimestamp()});
    try chanOfChan.send(chan);
}
