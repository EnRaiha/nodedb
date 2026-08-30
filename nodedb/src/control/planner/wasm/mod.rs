// SPDX-License-Identifier: BUSL-1.1

//! WASM UDF runtime.
//!
//! **Execution location:** WASM UDFs execute on the **Control Plane** (Tokio).
//! They are pure compute — no collection access, no DML, no transaction control.
//! The wasmtime JIT runs on the same thread pool as DataFusion query execution.
//! This is intentional: WASM UDFs are called from within DataFusion ScalarUDF
//! evaluation, which runs on the Control Plane.

mod config;
pub mod enforcement;
pub mod fuel;
pub mod pool;
pub mod runtime;
pub mod store;
pub mod types;
pub mod wit;

pub use config::WasmConfig;
