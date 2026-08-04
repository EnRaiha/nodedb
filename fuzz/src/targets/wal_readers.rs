use std::fs;
use std::path::Path;

#[cfg(target_os = "linux")]
use nodedb_wal::mmap_reader::MmapWalReader;
use nodedb_wal::reader::WalReader;
use nodedb_wal::record::{HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE};
use nodedb_wal::{
    LazyWalReader, PREAMBLE_SIZE, RecordHeader, RecordType, WAL_PREAMBLE_MAGIC, WalRecord,
    WalRecordArgs,
};

const MAX_WAL_FUZZ_INPUT: usize = 1024 * 1024;
const MAX_RECORDS: usize = 4;
const MAX_FUZZ_PAYLOAD: usize = 4096;
const PAYLOAD_LEN_OFFSET: usize = 30;
const PAYLOAD_LEN_END: usize = PAYLOAD_LEN_OFFSET + size_of::<u32>();

/// Retain arbitrary bounded input while preventing valid headers from causing
/// reader allocations larger than the fuzz target's small payload budget.
fn bounded_wal_bytes(data: &[u8]) -> Vec<u8> {
    let mut bytes = data[..data.len().min(MAX_WAL_FUZZ_INPUT)].to_vec();
    let mut offset = if bytes.len() >= PREAMBLE_SIZE && bytes.starts_with(&WAL_PREAMBLE_MAGIC) {
        PREAMBLE_SIZE
    } else {
        0
    };
    let mut records = 0;

    while records < MAX_RECORDS {
        let Some(header_end) = offset.checked_add(HEADER_SIZE) else {
            break;
        };
        if header_end > bytes.len() {
            break;
        }

        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&bytes[offset..header_end]);
        let header = RecordHeader::from_bytes(&header_bytes);
        if header.validate(offset as u64).is_err() {
            break;
        }

        let payload_len = header.payload_len as usize;
        if payload_len > MAX_FUZZ_PAYLOAD {
            bytes[offset + PAYLOAD_LEN_OFFSET..offset + PAYLOAD_LEN_END]
                .copy_from_slice(&((MAX_WAL_PAYLOAD_SIZE as u32).saturating_add(1)).to_le_bytes());
            bytes.truncate(header_end);
            break;
        }

        let Some(record_end) = header_end.checked_add(payload_len) else {
            break;
        };
        if record_end > bytes.len() {
            break;
        }
        offset = record_end;
        records += 1;
    }

    if records == MAX_RECORDS {
        bytes.truncate(offset);
    }
    bytes
}

fn append_record(bytes: &mut Vec<u8>, record_type: RecordType, lsn: u64, payload: Vec<u8>) -> bool {
    let Ok(record) = WalRecord::new(WalRecordArgs {
        record_type: record_type as u32,
        lsn,
        tenant_id: 7,
        vshard_id: 3,
        database_id: 11,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    }) else {
        return false;
    };

    bytes.extend_from_slice(&record.header.to_bytes());
    bytes.extend_from_slice(&record.payload);
    true
}

/// Build a short checksum-valid sequence through the public WAL constructor.
fn valid_wal_bytes(data: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = Vec::with_capacity((HEADER_SIZE + MAX_FUZZ_PAYLOAD) * 2);
    let fuzz_payload = data[..data.len().min(64)].to_vec();
    if !append_record(&mut bytes, RecordType::TimeseriesBatch, 1, fuzz_payload)
        || !append_record(&mut bytes, RecordType::Put, 2, b"reader-seed".to_vec())
    {
        return None;
    }
    Some(bytes)
}

fn exercise_readers(path: &Path, bytes: &[u8]) {
    if fs::write(path, bytes).is_err() {
        return;
    }

    if let Ok(mut reader) = WalReader::open(path, None) {
        for _ in 0..MAX_RECORDS {
            match reader.next_record() {
                Ok(Some(record)) => {
                    let _ = record.wire_size();
                }
                Ok(None) | Err(_) => break,
            }
        }
        let _ = reader.offset();
        let _ = reader.segment_preamble();
    }

    if let Ok(mut reader) = LazyWalReader::open(path, None) {
        for index in 0..MAX_RECORDS {
            let header = match reader.next_header() {
                Ok(Some(header)) => header,
                Ok(None) | Err(_) => break,
            };
            let result = if index % 2 == 0 {
                reader.read_payload(&header).map(|payload| payload.len())
            } else {
                reader.skip_payload(&header).map(|()| 0)
            };
            if result.is_err() {
                break;
            }
        }
        let _ = reader.offset();
    }

    #[cfg(target_os = "linux")]
    if let Ok(mut reader) = MmapWalReader::open(path, None) {
        let _ = reader.len();
        let _ = reader.is_empty();
        let _ = reader.madvise_state();
        for _ in 0..MAX_RECORDS {
            match reader.next_record() {
                Ok(Some(record)) => {
                    let _ = record.wire_size();
                }
                Ok(None) | Err(_) => break,
            }
        }
        let _ = reader.offset();
        reader.release_pages();
    }
}

pub fn run(data: &[u8]) {
    let bounded = bounded_wal_bytes(data);
    let mut header = [0u8; HEADER_SIZE];
    let header_len = bounded.len().min(header.len());
    header[..header_len].copy_from_slice(&bounded[..header_len]);
    let _ = RecordHeader::from_bytes(&header);

    let temp = tempfile::NamedTempFile::new();
    if let Ok(file) = temp {
        exercise_readers(file.path(), &bounded);
        if let Some(valid) = valid_wal_bytes(data) {
            exercise_readers(file.path(), &valid);
        }
    }
}
