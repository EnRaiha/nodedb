// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral API-key DDL family: CREATE / REVOKE / LIST / SHOW API KEY(S).

mod create;
mod manage;
mod parse;

pub use create::create_api_key;
pub use manage::{list_api_keys, revoke_api_key};
