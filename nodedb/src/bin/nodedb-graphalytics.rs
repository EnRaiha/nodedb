// SPDX-License-Identifier: BUSL-1.1

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1).map(PathBuf::from);
    let dataset = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: nodedb-graphalytics DATASET OUTPUT DATABASE"))?;
    let output = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: nodedb-graphalytics DATASET OUTPUT DATABASE"))?;
    let database = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: nodedb-graphalytics DATASET OUTPUT DATABASE"))?;
    if args.next().is_some() {
        anyhow::bail!("usage: nodedb-graphalytics DATASET OUTPUT DATABASE");
    }
    nodedb::graphalytics::run(&dataset, &output, &database)
}
