pub mod custom;
pub mod plain;

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpFormat {
    Plain,
    Custom,
}

/// Detect format by checking the first 5 bytes for PGDMP magic.
pub fn detect_format(header: &[u8]) -> Result<DumpFormat> {
    // If we have at least 5 bytes and they match PGDMP, it's custom format
    if header.len() >= 5 && &header[..5] == b"PGDMP" {
        Ok(DumpFormat::Custom)
    } else if !header.is_empty() && header.starts_with(b"PGDM") {
        // If we have partial match (e.g., "PGDM"), it's likely custom format
        // but we need to read more to be sure
        Ok(DumpFormat::Custom)
    } else {
        Ok(DumpFormat::Plain)
    }
}

/// PGDMP magic bytes
pub const MAGIC_HEADER: &[u8; 5] = b"PGDMP";
