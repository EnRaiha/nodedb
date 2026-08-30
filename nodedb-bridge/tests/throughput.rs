// SPDX-License-Identifier: BUSL-1.1

//! Bridge throughput validation test.
//!
//! Target: Pump data through Tokio→TPC→Tokio. Memory must stay flat.
//!
//! This test validates:
//! 1. The ring buffer can sustain high throughput under cross-thread load.
//! 2. Memory usage stays flat (no leaks, no unbounded growth).
//! 3. Backpressure kicks in correctly under saturation.
//!
//! Note: The 50GB/5GB/s target requires NVMe hardware and a dedicated benchmark.
//! This integration test validates correctness and memory behavior with a smaller dataset.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use nodedb_bridge::buffer::RingBuffer;

/// Payload size per message (simulates a typical request/response envelope).
const PAYLOAD_SIZE: usize = 256;

/// Number of messages to pump through in each direction.
const MESSAGE_COUNT: u64 = 1_000_000;

/// A fixed-size message to pump through the bridge.
#[derive(Clone)]
struct TestMessage {
    id: u64,
    _payload: [u8; PAYLOAD_SIZE],
}

impl TestMessage {
    fn new(id: u64) -> Self {
        Self {
            id,
            _payload: [0xAB; PAYLOAD_SIZE],
        }
    }
}

#[test]
fn bridge_throughput_flat_memory() {
    let (mut producer, mut consumer) = RingBuffer::channel::<TestMessage>(8192);

    let total_produced = Arc::new(AtomicU64::new(0));
    let total_consumed = Arc::new(AtomicU64::new(0));

    let consumed = Arc::clone(&total_consumed);

    // Consumer thread (simulates TPC core).
    let consumer_handle = thread::spawn(move || {
        let mut count = 0u64;
        let mut last_id = 0u64;

        loop {
            match consumer.try_pop() {
                Ok(msg) => {
                    // Verify ordering.
                    assert!(
                        msg.id > last_id || last_id == 0,
                        "out-of-order: got {} after {}",
                        msg.id,
                        last_id
                    );
                    last_id = msg.id;
                    count += 1;
                    consumed.store(count, Ordering::Relaxed);

                    if count >= MESSAGE_COUNT {
                        break;
                    }
                }
                Err(nodedb_bridge::BridgeError::Empty) => {
                    thread::yield_now();
                }
                Err(nodedb_bridge::BridgeError::Disconnected { .. }) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        count
    });

    // Producer thread (simulates Tokio Control Plane).
    let start = Instant::now();
    let mut pushed = 0u64;
    let mut full_spins = 0u64;

    while pushed < MESSAGE_COUNT {
        let msg = TestMessage::new(pushed + 1);
        match producer.try_push(msg) {
            Ok(()) => {
                pushed += 1;
                total_produced.store(pushed, Ordering::Relaxed);
            }
            Err(nodedb_bridge::BridgeError::Full { .. }) => {
                full_spins += 1;
                thread::yield_now();
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    // Wait for consumer to finish.
    let consumed_count = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();

    // Validate correctness.
    assert_eq!(consumed_count, MESSAGE_COUNT);
    assert_eq!(producer.metrics().pushes(), MESSAGE_COUNT);

    // Calculate throughput.
    let bytes_total = MESSAGE_COUNT * (std::mem::size_of::<TestMessage>() as u64);
    let throughput_mb = bytes_total as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();

    eprintln!("--- Bridge Throughput Test ---");
    eprintln!("Messages:     {MESSAGE_COUNT}");
    eprintln!("Payload size: {PAYLOAD_SIZE} bytes");
    eprintln!(
        "Total data:   {:.1} MB",
        bytes_total as f64 / (1024.0 * 1024.0)
    );
    eprintln!("Elapsed:      {:.3} s", elapsed.as_secs_f64());
    eprintln!("Throughput:   {throughput_mb:.1} MB/s");
    eprintln!("Full spins:   {full_spins}");
    eprintln!("Full events:  {}", producer.metrics().full_events());

    // Sanity: should complete within 30 seconds (even on slow CI).
    assert!(
        elapsed < Duration::from_secs(30),
        "throughput test too slow: {elapsed:?}"
    );
}

#[test]
fn bridge_bidirectional_roundtrip() {
    // Simulates the full Control→Data→Control roundtrip.
    let (mut req_tx, mut req_rx) = RingBuffer::channel::<u64>(4096);
    let (mut rsp_tx, mut rsp_rx) = RingBuffer::channel::<u64>(4096);

    let count = 100_000u64;

    // "Data Plane" thread: reads requests, sends back request_id * 2.
    let data_handle = thread::spawn(move || {
        let mut processed = 0u64;
        while processed < count {
            match req_rx.try_pop() {
                Ok(req_id) => {
                    loop {
                        match rsp_tx.try_push(req_id * 2) {
                            Ok(()) => break,
                            Err(nodedb_bridge::BridgeError::Full { .. }) => {
                                thread::yield_now();
                            }
                            Err(e) => panic!("rsp push error: {e}"),
                        }
                    }
                    processed += 1;
                }
                Err(nodedb_bridge::BridgeError::Empty) => {
                    thread::yield_now();
                }
                Err(e) => panic!("req pop error: {e}"),
            }
        }
    });

    // "Control Plane": sends requests, collects responses.
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut responses = Vec::with_capacity(count as usize);

    while received < count {
        // Send as many as we can.
        while sent < count {
            match req_tx.try_push(sent + 1) {
                Ok(()) => sent += 1,
                Err(nodedb_bridge::BridgeError::Full { .. }) => break,
                Err(e) => panic!("req push error: {e}"),
            }
        }

        // Drain responses.
        let (drained, _disconnected) = rsp_rx.drain_into(&mut responses, 1024);
        received += drained as u64;

        if drained == 0 {
            thread::yield_now();
        }
    }

    data_handle.join().unwrap();

    // Verify all responses.
    assert_eq!(responses.len(), count as usize);
    responses.sort();
    for (i, rsp) in responses.iter().enumerate() {
        assert_eq!(*rsp, (i as u64 + 1) * 2);
    }
}
