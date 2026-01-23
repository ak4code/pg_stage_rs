use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use crate::error::{PgStageError, Result};
use crate::format::custom::header::CompressionMethod;
use crate::format::custom::io::DumpIO;
use crate::processor::DataProcessor;

const OUTPUT_CHUNK_SIZE: usize = 512 * 1024; // 512KB
const MAX_CHUNK_SIZE: usize = 50 * 1024 * 1024; // 50MB

/// Processes data blocks in a custom format dump.
pub struct BlockProcessor<'a> {
    dio: &'a DumpIO,
    compression: CompressionMethod,
    processor: &'a mut DataProcessor,
}

impl<'a> BlockProcessor<'a> {
    pub fn new(
        dio: &'a DumpIO,
        compression: CompressionMethod,
        processor: &'a mut DataProcessor,
    ) -> Self {
        Self {
            dio,
            compression,
            processor,
        }
    }

    /// Process a data block: read chunks, decompress if needed, mutate, compress, write.
    pub fn process_block<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Read and decompress all chunks into one buffer
        let mut data = Vec::new();
        let mut input_was_compressed = false;

        loop {
            let chunk_len = self.dio.read_int(reader)?;

            if chunk_len == 0 {
                break;
            }

            let len = chunk_len.unsigned_abs() as usize;

            // Validation
            if len > MAX_CHUNK_SIZE {
                return Err(PgStageError::InvalidFormat(format!(
                    "Chunk size {} exceeds maximum {}",
                    len, MAX_CHUNK_SIZE
                )));
            }

            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;

            if chunk_len < 0 {
                // Negative length = compressed data (standard pg_dump behavior)
                input_was_compressed = true;
                let decompressed = self.decompress_chunk(&buf)?;
                data.extend_from_slice(&decompressed);
            } else {
                // Positive length = uncompressed data
                data.extend_from_slice(&buf);
            }
        }

        if data.is_empty() {
            // Empty block - write terminator only
            self.dio.write_int(writer, 0)?;
            return Ok(());
        }

        // Process lines (mutation)
        let processed = self.process_lines(&data)?;

        // Determine if we should compress the output.
        // custom.py logic: if compression is ZLIB or RAW (which maps to Zlib behavior),
        // it strictly compresses the output using zlib level 6.
        if self.compression == CompressionMethod::Zlib {
            self.compress_and_write(writer, &processed)?;
        } else if self.compression == CompressionMethod::None {
             self.write_uncompressed(writer, &processed)?;
        } else {
             // Fallback for Lz4/Zstd (not fully supported for write in this snippet).
             // If input was compressed, we re-compress with Zlib to be safe/compact,
             // otherwise uncompressed.
             if input_was_compressed {
                 self.compress_and_write(writer, &processed)?;
             } else {
                 self.write_uncompressed(writer, &processed)?;
             }
        }

        // Terminator
        self.dio.write_int(writer, 0)?;

        Ok(())
    }

    /// Pass through a block without mutation.
    pub fn pass_through_block<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        loop {
            let chunk_len = self.dio.read_int(reader)?;
            self.dio.write_int(writer, chunk_len)?;

            if chunk_len == 0 {
                break;
            }

            let len = chunk_len.unsigned_abs() as usize;

            if len > MAX_CHUNK_SIZE {
                return Err(PgStageError::InvalidFormat(format!(
                    "Chunk size {} exceeds maximum {}, stream may be corrupted",
                    len, MAX_CHUNK_SIZE
                )));
            }

            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            writer.write_all(&buf)?;
        }
        Ok(())
    }

    fn decompress_chunk(&self, compressed: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = ZlibDecoder::new(compressed);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| PgStageError::CompressionError(format!("Zlib decompression failed: {}", e)))?;
        Ok(decompressed)
    }

    fn process_lines(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(data.len());
        let mut start = 0;

        while start < data.len() {
            // Find end of line
            let end = data[start..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|p| start + p)
                .unwrap_or(data.len());

            let line = &data[start..end];

            if let Some(mutated) = self.processor.process_line(line) {
                result.extend_from_slice(&mutated);
                // Python custom.py _process_line_bytes adds \n if it wasn't empty result
                // We add it if the original had it (standard pg COPY behavior)
                // or if we are just rebuilding the block.
                if end < data.len() {
                    result.push(b'\n');
                }
            }

            start = if end < data.len() { end + 1 } else { end };
        }

        Ok(result)
    }

    fn write_uncompressed<W: Write>(&self, writer: &mut W, data: &[u8]) -> Result<()> {
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + OUTPUT_CHUNK_SIZE).min(data.len());
            let chunk = &data[offset..end];
            // Positive length = uncompressed data
            self.dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;
            offset = end;
        }
        Ok(())
    }

    fn compress_and_write<W: Write>(&self, writer: &mut W, data: &[u8]) -> Result<()> {
        // Python uses zlib level 6
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
        encoder
            .write_all(data)
            .map_err(|e| PgStageError::CompressionError(format!("Zlib compression failed: {}", e)))?;
        let compressed = encoder
            .finish()
            .map_err(|e| PgStageError::CompressionError(format!("Zlib compression finish failed: {}", e)))?;

        // Write compressed data in chunks
        let mut offset = 0;
        while offset < compressed.len() {
            let end = (offset + OUTPUT_CHUNK_SIZE).min(compressed.len());
            let chunk = &compressed[offset..end];

            // CRITICAL: custom.py writes POSITIVE integers for the length of compressed chunks!
            // DumpIO::write_int writes [sign_byte, bytes...].
            // Passing a positive length results in sign_byte 0.
            // Standard pg_dump usually writes negative, but custom.py writes positive.
            self.dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;

            offset = end;
        }

        Ok(())
    }
}