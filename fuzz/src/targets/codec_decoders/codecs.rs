use nodedb_codec::codec_types::ColumnCodec;

pub(crate) fn column_codecs() -> [ColumnCodec; 16] {
    [
        ColumnCodec::Auto,
        ColumnCodec::AlpFastLanesLz4,
        ColumnCodec::AlpRdLz4,
        ColumnCodec::PcodecLz4,
        ColumnCodec::DeltaFastLanesLz4,
        ColumnCodec::FastLanesLz4,
        ColumnCodec::FsstLz4,
        ColumnCodec::AlpFastLanesRans,
        ColumnCodec::DeltaFastLanesRans,
        ColumnCodec::FsstRans,
        ColumnCodec::Gorilla,
        ColumnCodec::DoubleDelta,
        ColumnCodec::Delta,
        ColumnCodec::Lz4,
        ColumnCodec::Zstd,
        ColumnCodec::Raw,
    ]
}
