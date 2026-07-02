// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL family handlers + router.
//!
//! Handlers here build [`DdlResult`] / [`DdlError`] directly, carrying no
//! pgwire types. [`try_dispatch`] recognizes the migrated families and routes
//! to them; every other statement returns `None` so the transitional pgwire
//! delegation in the parent [`super::dispatch`] handles it.

pub mod oidc;
pub mod rls;
pub mod router;
pub mod sequence;

pub use self::router::try_dispatch;
