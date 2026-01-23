use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use crate::error::{PgStageError, Result};
use crate::format::custom::header::CompressionMethod;
use crate::format::custom::io::DumpIO;
use crate::processor::DataProcessor;

const OUTPUT_CHUNK_SIZE: usize = 512 * 1024; // 512KB

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

    /// Process a data block: read chunks, decompress, mutate, compress, write.
    pub fn process_block<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        match self.compression {
            CompressionMethod::Zlib | CompressionMethod::Raw => {
                self.process_compressed_block(reader, writer)
            }
            CompressionMethod::None => self.process_uncompressed_block(reader, writer),
            CompressionMethod::Lz4 => Err(PgStageError::CompressionError(
                "LZ4 compression not yet supported".to_string(),
            )),
        }
    }

    /// Pass through a block without mutation (for non-data or unmutated blocks).
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
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            writer.write_all(&buf)?;
        }
        Ok(())
    }

    fn process_compressed_block<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Read all compressed chunks into a buffer
        let mut compressed_data = Vec::new();
        loop {
            let chunk_len = self.dio.read_int(reader)?;
            if chunk_len == 0 {
                break;
            }
            let len = chunk_len.unsigned_abs() as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            compressed_data.extend_from_slice(&buf);
        }

        if compressed_data.is_empty() {
            // Write terminator
            self.dio.write_int(writer, 0)?;
            return Ok(());
        }

        // Decompress
        let decompressed = self.decompress_data(&compressed_data)?;

        // Process lines
        let processed = self.process_lines(&decompressed)?;

        // Compress and write in chunks
        self.compress_and_write(writer, &processed)?;

        // Write terminator chunk (length = 0)
        self.dio.write_int(writer, 0)?;

        Ok(())
    }

    fn process_uncompressed_block<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Read all chunks
        let mut data = Vec::new();
        loop {
            let chunk_len = self.dio.read_int(reader)?;
            if chunk_len == 0 {
                break;
            }
            let len = chunk_len.unsigned_abs() as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            data.extend_from_slice(&buf);
        }

        if data.is_empty() {
            self.dio.write_int(writer, 0)?;
            return Ok(());
        }

        // Process lines
        let processed = self.process_lines(&data)?;

        // Write in chunks (uncompressed)
        let mut offset = 0;
        while offset < processed.len() {
            let end = (offset + OUTPUT_CHUNK_SIZE).min(processed.len());
            let chunk = &processed[offset..end];
            self.dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;
            offset = end;
        }

        // Write terminator
        self.dio.write_int(writer, 0)?;

        Ok(())
    }

    fn decompress_data(&self, compressed: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = ZlibDecoder::new(compressed);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            PgStageError::CompressionError(format!("Zlib decompression failed: {}", e))
        })?;
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
                if end < data.len() {
                    result.push(b'\n');
                }
            }
            // If None, the line is deleted (table marked for deletion)

            start = if end < data.len() { end + 1 } else { end };
        }

        Ok(result)
    }

    fn compress_and_write<W: Write>(&self, writer: &mut W, data: &[u8]) -> Result<()> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
        encoder.write_all(data).map_err(|e| {
            PgStageError::CompressionError(format!("Zlib compression failed: {}", e))
        })?;
        let compressed = encoder.finish().map_err(|e| {
            PgStageError::CompressionError(format!("Zlib compression finish failed: {}", e))
        })?;

        // Write compressed data in chunks
        let mut offset = 0;
        while offset < compressed.len() {
            let end = (offset + OUTPUT_CHUNK_SIZE).min(compressed.len());
            let chunk = &compressed[offset..end];
            self.dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;
            offset = end;
        }

        Ok(())
    }
}
