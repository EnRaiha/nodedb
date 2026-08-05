// SPDX-License-Identifier: BUSL-1.1

pub mod builder;
pub mod resolved;
pub mod stores;

pub use builder::RequestAuthScopeBuilder;
pub use resolved::RequestAuthScope;
pub use stores::AuthStores;
