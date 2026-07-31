// SPDX-License-Identifier: BUSL-1.1

//! Per-protocol `NODEDB_TLS_*` toggles. No-op if the config has no `[tls]`
//! section at all — TLS is enabled/disabled per protocol, not created here.

use super::helpers::apply_bool_env;
use crate::config::server::ServerConfig;

pub(super) fn apply_tls_overrides(config: &mut ServerConfig) {
    if let Some(ref mut tls) = config.server.tls {
        apply_bool_env("NODEDB_TLS_NATIVE", &mut tls.native);
        apply_bool_env("NODEDB_TLS_PGWIRE", &mut tls.pgwire);
        apply_bool_env("NODEDB_TLS_HTTP", &mut tls.http);
        apply_bool_env("NODEDB_TLS_RESP", &mut tls.resp);
        apply_bool_env("NODEDB_TLS_ILP", &mut tls.ilp);
    }
}
