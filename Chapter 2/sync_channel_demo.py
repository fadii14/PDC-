"""
Parallel & Distributed Computing - Assignment 2
Focus : Coordinating producer/consumer threads over a capped channel
Source: Chapter 2 (Thread-based Parallelism) - Python Parallel Programming Cookbook

How this differs from the plain textbook recipe:
  - The shared list is replaced by a SyncChannel object that mediates every
    hand-off, so the locking rules live in exactly one place.
  - A pair of counting semaphores models free vs. occupied capacity, which makes
    suppliers wait on a full channel and collectors wait on an empty one without
    any busy looping.
  - A mutex keeps each push/pop on the channel atomic.
  - A threading.Event, paired with a short timed wait, tells collectors to drain
    whatever remains and then close down without hanging.
"""

import threading
import time
import random
from collections import deque
from datetime import datetime

# ---- Adjustable settings -----------------------------------------------------
CHANNEL_SIZE = 10     # maximum packets held in the channel at any instant
TOTAL_PACKETS = 20    # overall number of packets suppliers will generate


def record(tag: str, phase: str, note: str) -> None:
    """Emit a single millisecond-stamped line for one thread event."""
    stamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{stamp}   [{tag:^11}]   {phase:<10}   ::   {note}")


class SyncChannel:
    """
    A capacity-limited, thread-safe hand-off point.

    Everything about concurrency is hidden in here:
      * _free   - semaphore counting open positions (suppliers wait on it)
      * _filled - semaphore counting pending packets (collectors wait on it)
      * _lock   - mutex guarding the backing deque
      * _closed - event raised once no further packets will be offered
    """

    def __init__(self, size: int):
        self.size = size
        self._backing = deque()
        self._lock = threading.Lock()
        self._free = threading.Semaphore(size)
        self._filled = threading.Semaphore(0)
        self._closed = threading.Event()

    def offer(self, packet: str, who: str) -> None:
        """Wait for room, then push a packet onto the channel atomically."""
        self._free.acquire()
        with self._lock:
            self._backing.append(packet)
            record(who, "PUSHED", f"{packet}   [fill {len(self._backing)}/{self.size}]")
        self._filled.release()

    def poll(self, who: str):
        """Pull one packet if available; returns None on timeout/empty channel."""
        if not self._filled.acquire(timeout=0.5):
            return None

        packet = None
        with self._lock:
            if self._backing:
                packet = self._backing.popleft()
                record(who, "PULLED", f"{packet}   [fill {len(self._backing)}/{self.size}]")

        if packet is not None:
            self._free.release()
        return packet

    def seal(self) -> None:
        """Mark the channel as closed so collectors know to finish up."""
        self._closed.set()

    @property
    def sealed(self) -> bool:
        return self._closed.is_set()

    def __len__(self) -> int:
        return len(self._backing)


def supplier(channel: SyncChannel, sid: int, quota: int) -> None:
    """Create `quota` packets and feed them into the channel."""
    tag = f"Supplier-{sid}"
    record(tag, "STARTUP", "thread is live")

    for n in range(quota):
        packet = f"S{sid}-pkt{n + 1}"
        time.sleep(random.uniform(0.1, 0.4))   # stand-in for real work
        channel.offer(packet, tag)

    record(tag, "CLOSED", "quota met")


def collector(channel: SyncChannel, cid: int) -> None:
    """Drain packets from the channel until it is both sealed and empty."""
    tag = f"Collector-{cid}"
    record(tag, "STARTUP", "thread is live")

    while not channel.sealed or len(channel) > 0:
        packet = channel.poll(tag)
        if packet is None:
            continue
        time.sleep(random.uniform(0.2, 0.5))   # stand-in for processing

    record(tag, "CLOSED", "channel drained")


def main() -> None:
    rule = "=" * 80
    print(rule)
    print("  PDC ASSIGNMENT 2  ::  Capped-Channel Supplier / Collector Coordination")
    print(f"  Channel size = {CHANNEL_SIZE}   |   Packets to move = {TOTAL_PACKETS}")
    print(rule)

    t0 = time.time()
    channel = SyncChannel(CHANNEL_SIZE)

    supplier_n = 4
    collector_n = 2
    quota = TOTAL_PACKETS // supplier_n

    suppliers = [
        threading.Thread(target=supplier, args=(channel, i + 1, quota), name=f"Supplier-{i + 1}")
        for i in range(supplier_n)
    ]
    collectors = [
        threading.Thread(target=collector, args=(channel, i + 1), name=f"Collector-{i + 1}")
        for i in range(collector_n)
    ]

    # Collectors start first so they are ready the moment packets arrive.
    for c in collectors:
        c.start()
    for s in suppliers:
        s.start()

    # When every supplier has met its quota, seal the channel.
    for s in suppliers:
        s.join()
    channel.seal()

    for c in collectors:
        c.join()

    print(rule)
    print(f"  Elapsed : {time.time() - t0:.2f} s")
    print(f"  Channel remainder : {len(channel)} packet(s)")
    print(rule)


if __name__ == "__main__":
    main()
