use std::io::{Read, Write};

use crate::error::{PgStageError, Result};
use crate::format::custom::io::DumpIO;
use crate::format::MAGIC_HEADER;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    None,
    Zlib,
    Lz4,
    Zstd,
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
pub fn parse_header<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    initial_bytes: &[u8],
) -> Result<Header> {
    // Write initial bytes (the magic we already consumed for detection)
    writer.write_all(initial_bytes)?;

    // Read remaining magic if initial_bytes is partial
    let magic_remaining = MAGIC_HEADER.len().saturating_sub(initial_bytes.len());
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
    } else if initial_bytes.len() >= MAGIC_HEADER.len() {
        // Validate magic prefix
        if &initial_bytes[..MAGIC_HEADER.len()] != MAGIC_HEADER {
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

    #[cfg(debug_assertions)]
    eprintln!("[DEBUG] pg_dump format version: {}.{}.{}", vmaj, vmin, vrev);

    // custom.py validation: < 1.12 or > 1.16 is unsupported
    if vmaj < 1 || (vmaj == 1 && vmin < 12) {
        return Err(PgStageError::UnsupportedVersion(format!(
            "Version {}.{}.{} is too old (min 1.12.0)",
            vmaj, vmin, vrev
        )));
    }
    if vmaj > 1 || (vmaj == 1 && vmin > 16) {
        return Err(PgStageError::UnsupportedVersion(format!(
            "Version {}.{}.{} is too new (max 1.16.0)",
            vmaj, vmin, vrev
        )));
    }

    // Integer size
    let int_size = DumpIO::read_byte(reader)? as usize;
    writer.write_all(&[int_size as u8])?;

    // Offset size
    let offset_size = DumpIO::read_byte(reader)? as usize;
    writer.write_all(&[offset_size as u8])?;

    #[cfg(debug_assertions)]
    eprintln!("[DEBUG] int_size={}, offset_size={}", int_size, offset_size);

    // Validate sizes
    if int_size == 0 || int_size > 8 || offset_size == 0 || offset_size > 8 {
        return Err(PgStageError::InvalidFormat(format!(
            "Invalid int_size={} or offset_size={}", int_size, offset_size
        )));
    }

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

    // Compression method
    let compression = if (vmaj, vmin, vrev) >= (1, 15, 0) {
        // v1.15+: 1 byte compression algorithm.
        // NOTE: custom.py does NOT read the integer level following this byte for >= 1.15.
        // It strictly reads 1 byte and maps it. Reading an extra int here causes desync.
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        writer.write_all(&buf)?;
        let compression_algo = buf[0];

        match compression_algo {
            0 => CompressionMethod::None,
            1 => CompressionMethod::Zlib, // custom.py calls this RAW but maps to zlib behavior
            2 => CompressionMethod::Lz4,
            3 => CompressionMethod::Zstd, // custom.py calls this ZLIB
            other => {
                return Err(PgStageError::InvalidFormat(format!(
                    "Unknown compression algorithm byte {}",
                    other
                )));
            }
        }
    } else {
        // Pre-1.15: compression field is the zlib level directly
        // 0 = no compression
        // -1 = default zlib (level 6)
        // 1-9 = zlib with that level
        let level = dio.read_int_bypass(reader, writer)?;

        if level == 0 {
            CompressionMethod::None
        } else if level == -1 || (1..=9).contains(&level) {
            CompressionMethod::Zlib
        } else {
            return Err(PgStageError::InvalidFormat(format!(
                "Invalid compression level {}",
                level
            )));
        }
    };

    #[cfg(debug_assertions)]
    eprintln!("[DEBUG] Compression: {:?}", compression);

    // Timestamp: custom.py reads 7 integers (sec, min, hour, mday, mon, year, isdst)
    // The 7th integer is ignored in Python (_isdst), but must be read/written to maintain sync.
    for _ in 0..7 {
        dio.read_int_bypass(reader, writer)?;
    }

    // Database name (string)
    let _db_name = dio.read_string_bypass(reader, writer)?;
    #[cfg(debug_assertions)]
    eprintln!("Database: {:?}", _db_name);

    // Server version (string)
    let _server_ver = dio.read_string_bypass(reader, writer)?;
    #[cfg(debug_assertions)]
    eprintln!("Server version: {:?}", _server_ver);

    // Dump version (string)
    let _dump_ver = dio.read_string_bypass(reader, writer)?;
    #[cfg(debug_assertions)]
    eprintln!("pg_dump version string: {:?}", _dump_ver);

    Ok(Header {
        vmaj,
        vmin,
        vrev,
        int_size,
        offset_size,
        format,
        compression,
    })
}