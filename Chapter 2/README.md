# Parallel & Distributed Computing — Assignment 2
**Focus:** Thread-based Parallelism & Synchronization (Chapter 2)
**Program:** A capped supplier/collector channel coordinated by counting semaphores and a mutex

---

## 1. Summary
A coursework submission for the Parallel & Distributed Computing module, built on the
thread-synchronization material in **Chapter 2** of the *Python Parallel Programming Cookbook
(Second Edition)*, Packt Publishing.

The book's recipe only sketches the idea with a `threading.Semaphore` and a list that grows
without limit. This program turns it into a proper **capped channel**: several supplier threads
push packets in while several collector threads pull them out, all without overflow, underflow,
or data races on the shared container.

---

## 2. Design notes
1. **A single channel object holds the concurrency.**
   All locking and signalling sit inside the `SyncChannel` class, so the supplier and collector
   functions never touch a semaphore directly — the critical section is in one spot.
2. **Semaphores instead of guesswork sleeps.**
   - `_free` begins at `CHANNEL_SIZE (10)` and counts open positions, so suppliers block as soon
     as the channel is full.
   - `_filled` begins at `0` and counts pending packets, so collectors block as soon as the
     channel is empty.
3. **Mutex on every change.**
   A `threading.Lock` wraps each push/pop on the internal `deque`, keeping the update atomic.
4. **Orderly shutdown.**
   A `threading.Event` plus a half-second timed `acquire` lets collectors clear any leftover
   packets and exit on their own, with no deadlock.
5. **Clear tracing.**
   Each event prints a millisecond timestamp, the thread tag, the phase, and the live fill level.

---

## 3. Flow at a glance

```

[Supplier-1 .. Supplier-4]
        |
        v   offer()  -> acquire(_free)
+--------------------------------------------------+
|             MUTEX  (_lock)                        |
|     deque channel  [ size = 10 packets ]          |
+--------------------------------------------------+
        ^   poll()   -> acquire(_filled)
        |
[Collector-1  Collector-2]

```

---

## 4. Running it
Needs Python 3.8 or newer:

```bash
cd "Chapter 2"
python sync_channel_demo.py
```

### Sample trace (timings vary between runs)

```text
================================================================================
  PDC ASSIGNMENT 2  ::  Capped-Channel Supplier / Collector Coordination
  Channel size = 10   |   Packets to move = 20
================================================================================
19:51:12.750   [Collector-1]   STARTUP      ::   thread is live
19:51:12.751   [Collector-2]   STARTUP      ::   thread is live
19:51:12.754   [Supplier-1 ]   STARTUP      ::   thread is live
19:51:12.858   [Supplier-2 ]   PUSHED       ::   S2-pkt1   [fill 1/10]
19:51:12.858   [Collector-1]   PULLED       ::   S2-pkt1   [fill 0/10]
19:51:13.016   [Supplier-4 ]   PUSHED       ::   S4-pkt1   [fill 1/10]
19:51:13.016   [Collector-2]   PULLED       ::   S4-pkt1   [fill 0/10]
...
19:51:16.410   [Collector-1]   CLOSED       ::   channel drained
19:51:16.433   [Collector-2]   CLOSED       ::   channel drained
================================================================================
  Elapsed : 3.68 s
  Channel remainder : 0 packet(s)
================================================================================
```
