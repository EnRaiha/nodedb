# `nodedb-columnar`

> Compressed columnar segment format

NDBS-format segments with versioned footer and CRC32C, per-column codec selection, 1024-row blocks with statistics for predicate pushdown, roaring delete bitmaps, and single-segment compaction for embedded deployments. Shared by the columnar / timeseries / spatial engines.

## Status

Pre-1.0. APIs may change between minor versions until 1.0. See the
workspace [`README.md`](../README.md) for the full project overview and
the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases)
for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
