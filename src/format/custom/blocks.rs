use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

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
        match self.compression {
            CompressionMethod::Zlib => self.process_block_zlib(reader, writer),
            CompressionMethod::Zstd => self.process_block_zstd(reader, writer),
            CompressionMethod::None | CompressionMethod::Lz4 => {
                self.process_block_uncompressed(reader, writer)
            }
        }
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

    /// Streaming processing for uncompressed blocks.
    /// Reads chunks one at a time, processes complete lines, writes output immediately.
    fn process_block_uncompressed<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        let mut line_tail: Vec<u8> = Vec::new();
        let mut output_buf: Vec<u8> = Vec::with_capacity(OUTPUT_CHUNK_SIZE * 2);

        loop {
            let chunk_len = self.dio.read_int(reader)?;
            if chunk_len == 0 {
                break;
            }

            let len = chunk_len.unsigned_abs() as usize;
            if len > MAX_CHUNK_SIZE {
                return Err(PgStageError::InvalidFormat(format!(
                    "Chunk size {} exceeds maximum {}",
                    len, MAX_CHUNK_SIZE
                )));
            }

            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;

            // Prepend leftover from previous chunk
            let data = if line_tail.is_empty() {
                buf
            } else {
                let mut combined = std::mem::take(&mut line_tail);
                combined.extend_from_slice(&buf);
                combined
            };

            // Split into complete lines + tail
            match data.iter().rposition(|&b| b == b'\n') {
                Some(last_nl) => {
                    line_tail = data[last_nl + 1..].to_vec();
                    self.process_complete_lines(&data[..=last_nl], &mut output_buf);

                    // Flush output when large enough
                    if output_buf.len() >= OUTPUT_CHUNK_SIZE {
                        self.flush_uncompressed(writer, &mut output_buf)?;
                    }
                }
                None => {
                    line_tail = data;
                }
            }
        }

        // Process remaining tail (last line without newline)
        if !line_tail.is_empty() {
            if let Some(mutated) = self.processor.process_line(&line_tail) {
                output_buf.extend_from_slice(&mutated);
            }
        }

        // Write remaining output
        if !output_buf.is_empty() {
            self.flush_uncompressed(writer, &mut output_buf)?;
        }

        // Terminator
        self.dio.write_int(writer, 0)?;
        Ok(())
    }

    /// Streaming processing for zlib-compressed blocks.
    /// Reads all compressed chunks (they form one zlib stream), then decompresses
    /// and processes incrementally.
    fn process_block_zlib<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Read all compressed chunks (small compared to decompressed size)
        let mut raw = Vec::new();
        loop {
            let chunk_len = self.dio.read_int(reader)?;
            if chunk_len == 0 {
                break;
            }

            let len = chunk_len.unsigned_abs() as usize;
            if len > MAX_CHUNK_SIZE {
                return Err(PgStageError::InvalidFormat(format!(
                    "Chunk size {} exceeds maximum {}",
                    len, MAX_CHUNK_SIZE
                )));
            }

            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            raw.extend_from_slice(&buf);
        }

        if raw.is_empty() {
            self.dio.write_int(writer, 0)?;
            return Ok(());
        }

        // Stream: decompress → process lines → compress → write chunks
        let mut decoder = ZlibDecoder::new(raw.as_slice());
        let mut encoder = ZlibEncoder::new(Vec::with_capacity(OUTPUT_CHUNK_SIZE), Compression::new(6));

        let mut read_buf = vec![0u8; OUTPUT_CHUNK_SIZE];
        let mut line_tail: Vec<u8> = Vec::new();

        loop {
            let n = decoder.read(&mut read_buf)
                .map_err(|e| PgStageError::CompressionError(format!("Zlib decompression failed: {}", e)))?;
            if n == 0 {
                break;
            }

            let data = if line_tail.is_empty() {
                &read_buf[..n]
            } else {
                line_tail.extend_from_slice(&read_buf[..n]);
                line_tail.as_slice()
            };

            match data.iter().rposition(|&b| b == b'\n') {
                Some(last_nl) => {
                    let complete = &data[..=last_nl];
                    let tail = &data[last_nl + 1..];

                    // Process complete lines directly into encoder
                    self.process_complete_lines_to_writer(complete, &mut encoder)?;

                    line_tail = tail.to_vec();

                    // Flush compressed output when large enough
                    self.flush_encoder_chunks(writer, &mut encoder)?;
                }
                None => {
                    if line_tail.is_empty() {
                        line_tail = read_buf[..n].to_vec();
                    }
                    // else: data already IS line_tail (extended above), keep as-is
                }
            }
        }

        // Process remaining tail
        if !line_tail.is_empty() {
            if let Some(mutated) = self.processor.process_line(&line_tail) {
                encoder.write_all(&mutated)
                    .map_err(|e| PgStageError::CompressionError(format!("Zlib compression failed: {}", e)))?;
            }
        }

        // Finalize encoder and write remaining compressed data
        let remaining = encoder.finish()
            .map_err(|e| PgStageError::CompressionError(format!("Zlib compression finish failed: {}", e)))?;
        if !remaining.is_empty() {
            for chunk in remaining.chunks(OUTPUT_CHUNK_SIZE) {
                self.dio.write_int(writer, chunk.len() as i32)?;
                writer.write_all(chunk)?;
            }
        }

        // Terminator
        self.dio.write_int(writer, 0)?;
        Ok(())
    }

    /// Streaming processing for zstd-compressed blocks.
    /// Reads all compressed chunks, decompresses, processes, and recompresses with zstd.
    fn process_block_zstd<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Read all compressed chunks
        let mut raw = Vec::new();
        loop {
            let chunk_len = self.dio.read_int(reader)?;
            if chunk_len == 0 {
                break;
            }

            let len = chunk_len.unsigned_abs() as usize;
            if len > MAX_CHUNK_SIZE {
                return Err(PgStageError::InvalidFormat(format!(
                    "Chunk size {} exceeds maximum {}",
                    len, MAX_CHUNK_SIZE
                )));
            }

            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            raw.extend_from_slice(&buf);
        }

        if raw.is_empty() {
            self.dio.write_int(writer, 0)?;
            return Ok(());
        }

        // Stream: decompress → process lines → compress → write chunks
        let mut decoder = ZstdDecoder::new(raw.as_slice())
            .map_err(|e| PgStageError::CompressionError(format!("Zstd decoder init failed: {}", e)))?;
        let mut encoder = ZstdEncoder::new(Vec::with_capacity(OUTPUT_CHUNK_SIZE), 3)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd encoder init failed: {}", e)))?;

        let mut read_buf = vec![0u8; OUTPUT_CHUNK_SIZE];
        let mut line_tail: Vec<u8> = Vec::new();

        loop {
            let n = decoder.read(&mut read_buf)
                .map_err(|e| PgStageError::CompressionError(format!("Zstd decompression failed: {}", e)))?;
            if n == 0 {
                break;
            }

            let data = if line_tail.is_empty() {
                &read_buf[..n]
            } else {
                line_tail.extend_from_slice(&read_buf[..n]);
                line_tail.as_slice()
            };

            match data.iter().rposition(|&b| b == b'\n') {
                Some(last_nl) => {
                    let complete = &data[..=last_nl];
                    let tail = &data[last_nl + 1..];

                    // Process complete lines directly into encoder
                    self.process_complete_lines_to_writer(complete, &mut encoder)?;

                    line_tail = tail.to_vec();

                    // Flush compressed output when large enough
                    self.flush_zstd_encoder_chunks(writer, &mut encoder)?;
                }
                None => {
                    if line_tail.is_empty() {
                        line_tail = read_buf[..n].to_vec();
                    }
                    // else: data already IS line_tail (extended above), keep as-is
                }
            }
        }

        // Process remaining tail
        if !line_tail.is_empty() {
            if let Some(mutated) = self.processor.process_line(&line_tail) {
                encoder.write_all(&mutated)
                    .map_err(|e| PgStageError::CompressionError(format!("Zstd compression failed: {}", e)))?;
            }
        }

        // Finalize encoder and write remaining compressed data
        let remaining = encoder.finish()
            .map_err(|e| PgStageError::CompressionError(format!("Zstd compression finish failed: {}", e)))?;
        if !remaining.is_empty() {
            for chunk in remaining.chunks(OUTPUT_CHUNK_SIZE) {
                self.dio.write_int(writer, chunk.len() as i32)?;
                writer.write_all(chunk)?;
            }
        }

        // Terminator
        self.dio.write_int(writer, 0)?;
        Ok(())
    }

    /// Process complete lines (each ending with \n) and append results to output buffer.
    fn process_complete_lines(&mut self, data: &[u8], output: &mut Vec<u8>) {
        let mut start = 0;
        while start < data.len() {
            let end = data[start..].iter().position(|&b| b == b'\n')
                .map(|p| start + p)
                .unwrap_or(data.len());

            let line = &data[start..end];
            if let Some(mutated) = self.processor.process_line(line) {
                output.extend_from_slice(&mutated);
                if end < data.len() {
                    output.push(b'\n');
                }
            }

            start = end + 1;
        }
    }

    /// Process complete lines and write results to a Write impl (encoder).
    fn process_complete_lines_to_writer<W: Write>(&mut self, data: &[u8], writer: &mut W) -> Result<()> {
        let mut start = 0;
        while start < data.len() {
            let end = data[start..].iter().position(|&b| b == b'\n')
                .map(|p| start + p)
                .unwrap_or(data.len());

            let line = &data[start..end];
            if let Some(mutated) = self.processor.process_line(line) {
                writer.write_all(&mutated)
                    .map_err(|e| PgStageError::CompressionError(format!("Write failed: {}", e)))?;
                if end < data.len() {
                    writer.write_all(b"\n")
                        .map_err(|e| PgStageError::CompressionError(format!("Write failed: {}", e)))?;
                }
            }

            start = end + 1;
        }
        Ok(())
    }

    /// Write all data in output_buf as uncompressed chunks and clear the buffer.
    fn flush_uncompressed<W: Write>(&self, writer: &mut W, output_buf: &mut Vec<u8>) -> Result<()> {
        for chunk in output_buf.chunks(OUTPUT_CHUNK_SIZE) {
            self.dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;
        }
        output_buf.clear();
        Ok(())
    }

    /// Flush accumulated compressed bytes from zlib encoder's inner buffer as chunks.
    fn flush_encoder_chunks<W: Write>(&self, writer: &mut W, encoder: &mut ZlibEncoder<Vec<u8>>) -> Result<()> {
        let inner = encoder.get_mut();
        if inner.len() >= OUTPUT_CHUNK_SIZE {
            for chunk in inner.chunks(OUTPUT_CHUNK_SIZE) {
                self.dio.write_int(writer, chunk.len() as i32)?;
                writer.write_all(chunk)?;
            }
            inner.clear();
        }
        Ok(())
    }

    /// Flush accumulated compressed bytes from zstd encoder's inner buffer as chunks.
    fn flush_zstd_encoder_chunks<W: Write>(&self, writer: &mut W, encoder: &mut ZstdEncoder<'_, Vec<u8>>) -> Result<()> {
        let inner = encoder.get_mut();
        if inner.len() >= OUTPUT_CHUNK_SIZE {
            for chunk in inner.chunks(OUTPUT_CHUNK_SIZE) {
                self.dio.write_int(writer, chunk.len() as i32)?;
                writer.write_all(chunk)?;
            }
            inner.clear();
        }
        Ok(())
    }
}