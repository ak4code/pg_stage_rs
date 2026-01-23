use std::io::{Read, Write};

use crate::error::{PgStageError, Result};
use crate::format::custom::io::DumpIO;
use crate::format::MAGIC_HEADER;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    None,
    Zlib,
    Raw,
    Lz4,
}

#[derive(Debug, Clone)]
pub struct Header {
    pub vmaj: u8,
    pub vmin: u8,
    pub vrev: u8,
    pub int_size: usize,
    pub offset_size: usize,
    pub format: u8,
    pub compression: CompressionMethod,
    pub compression_raw: i32,
}

impl Header {
    pub fn version_tuple(&self) -> (u8, u8, u8) {
        (self.vmaj, self.vmin, self.vrev)
    }

    pub fn is_version_at_least(&self, maj: u8, min: u8, rev: u8) -> bool {
        (self.vmaj, self.vmin, self.vrev) >= (maj, min, rev)
    }
}

/// Parse the header from a custom format dump.
/// Reads magic, version, int_size, offset_size, format, compression.
/// Also writes all read bytes to the bypass writer.
pub fn parse_header<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    initial_bytes: &[u8],
) -> Result<Header> {
    // Write initial bytes (the magic we already consumed for detection)
    writer.write_all(initial_bytes)?;

    // Read remaining magic if initial_bytes is partial, or validate if complete
    let magic_remaining = MAGIC_HEADER.len() - initial_bytes.len();
    if magic_remaining > 0 {
        let buf = DumpIO::read_exact(reader, magic_remaining)?;
        writer.write_all(&buf)?;
        // Validate combined magic
        let mut full_magic = initial_bytes.to_vec();
        full_magic.extend_from_slice(&buf);
        if full_magic != MAGIC_HEADER {
            return Err(PgStageError::InvalidFormat(
                "Invalid PGDMP magic header".to_string(),
            ));
        }
    } else if initial_bytes.len() == MAGIC_HEADER.len() {
        // If we already have full magic, validate it
        if initial_bytes != MAGIC_HEADER {
            return Err(PgStageError::InvalidFormat(
                "Invalid PGDMP magic header".to_string(),
            ));
        }
    }

    // Version: major.minor.rev
    let vmaj = DumpIO::read_byte(reader)?;
    writer.write_all(&[vmaj])?;
    let vmin = DumpIO::read_byte(reader)?;
    writer.write_all(&[vmin])?;
    let vrev = DumpIO::read_byte(reader)?;
    writer.write_all(&[vrev])?;

    if vmaj < 1 || (vmaj == 1 && vmin < 12) {
        return Err(PgStageError::UnsupportedVersion(format!(
            "{}.{}.{}",
            vmaj, vmin, vrev
        )));
    }

    // Integer size
    let int_size = DumpIO::read_byte(reader)? as usize;
    writer.write_all(&[int_size as u8])?;

    // Offset size
    let offset_size = DumpIO::read_byte(reader)? as usize;
    writer.write_all(&[offset_size as u8])?;

    // Format (should be 1 for custom)
    let format = DumpIO::read_byte(reader)?;
    writer.write_all(&[format])?;

    if format != 1 {
        return Err(PgStageError::InvalidFormat(format!(
            "Expected custom format (1), got {}",
            format
        )));
    }

    // Create DumpIO with parsed sizes
    let dio = DumpIO::new(int_size, offset_size);

    // Compression method (mirrors Python custom.py logic)
    let (compression, compression_raw) = if (vmaj, vmin, vrev) >= (1, 15, 0) {
        // v1.15+ uses a single compression method byte
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        writer.write_all(&buf)?;
        let compression_byte = buf[0];

        let method = match compression_byte {
            0 => CompressionMethod::None,
            1 => CompressionMethod::Raw,
            2 => CompressionMethod::Lz4,
            3 => CompressionMethod::Zlib,
            other => {
                return Err(PgStageError::InvalidFormat(format!(
                    "Unknown compression method byte {}",
                    other
                )));
            }
        };

        (method, i32::from(compression_byte))
    } else {
        // Pre-1.15: compression_raw is the zlib level
        // -1 -> ZLIB, 0 -> NONE, 1..9 -> RAW, otherwise invalid
        let level = dio.read_int_bypass(reader, writer)?;
        let method = if level == -1 {
            CompressionMethod::Zlib
        } else if level == 0 {
            CompressionMethod::None
        } else if (1..=9).contains(&level) {
            CompressionMethod::Raw
        } else {
            return Err(PgStageError::InvalidFormat(format!(
                "Invalid compression level {}",
                level
            )));
        };

        (method, level)
    };

    // sec (timestamp) - read and bypass
    dio.read_int_bypass(reader, writer)?;
    dio.read_int_bypass(reader, writer)?;
    dio.read_int_bypass(reader, writer)?;
    dio.read_int_bypass(reader, writer)?;
    dio.read_int_bypass(reader, writer)?;
    dio.read_int_bypass(reader, writer)?;

    // Database name (string)
    dio.read_string_bypass(reader, writer)?;
    // Server version (string)
    dio.read_string_bypass(reader, writer)?;
    // Dump version (string)
    dio.read_string_bypass(reader, writer)?;

    Ok(Header {
        vmaj,
        vmin,
        vrev,
        int_size,
        offset_size,
        format,
        compression,
        compression_raw,
    })
}
