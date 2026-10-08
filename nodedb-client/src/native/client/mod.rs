// SPDX-License-Identifier: Apache-2.0

pub mod core;
mod crdt_list;
mod dispatch;
mod document;
mod graph;
mod identity;
mod sql_lifecycle;
mod text_search;
mod vector;

pub use core::NativeClient;
