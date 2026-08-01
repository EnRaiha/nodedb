// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral consumer group DDL — CREATE / DROP / SHOW + COMMIT OFFSET(S).

pub mod commit;
pub mod create;
pub mod drop;
pub mod identity;
pub mod show;

pub use commit::commit_offset;
pub use create::create_consumer_group;
pub use drop::drop_consumer_group;
pub use show::{show_consumer_groups, show_partitions};
