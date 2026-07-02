// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL family handlers + router.
//!
//! Handlers here build [`DdlResult`] / [`DdlError`] directly, carrying no
//! pgwire types. [`try_dispatch`] recognizes the migrated families and routes
//! to them; every other statement returns `None` so the transitional pgwire
//! delegation in the parent [`super::dispatch`] handles it.

mod auth_support;
pub mod function;
pub mod grant;
pub mod oidc;
pub mod rls;
pub mod role;
pub mod router;
pub mod sequence;
pub mod service_account;
pub mod trigger;
pub mod user;

pub use self::router::try_dispatch;
