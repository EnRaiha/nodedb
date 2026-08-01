#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| nodedb_fuzz::targets::sql_preprocess::run(data));
