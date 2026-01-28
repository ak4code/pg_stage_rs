use std::io::{self, Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use memchr::memrchr;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::error::{PgStageError, Result};
use crate::format::custom::header::CompressionMethod;
use crate::format::custom::io::DumpIO;
use crate::processor::DataProcessor;

const OUTPUT_CHUNK_SIZE: usize = 1024 * 1024; // 1MB for better throughput
const MAX_CHUNK_SIZE: usize = 50 * 1024 * 1024; // 50MB
const READ_BUF_SIZE: usize = 2 * 1024 * 1024; // 2MB read buffer

/// Streaming reader that reads chunks on-demand instead of loading entire block into memory.
/// This is critical for large tables (100M+ rows) where compressed blocks can be several GB.
struct ChunkReader<'a, R: Read> {
    reader: &'a mut R,
    dio: &'a DumpIO,
    current_chunk: Vec<u8>,
    chunk_pos: usize,
    done: bool,
}

impl<'a, R: Read> ChunkReader<'a, R> {
    fn new(reader: &'a mut R, dio: &'a DumpIO) -> Self {
        Self {
            reader,
            dio,
            current_chunk: Vec::with_capacity(OUTPUT_CHUNK_SIZE),
            chunk_pos: 0,
            done: false,
        }
    }
}

impl<R: Read> Read for ChunkReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If current chunk exhausted, read next one
        if self.chunk_pos >= self.current_chunk.len() {
            if self.done {
                return Ok(0);
            }

            // Read next chunk length
            let chunk_len = self.dio.read_int(self.reader)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            if chunk_len == 0 {
                self.done = true;
                return Ok(0);
            }

            let len = chunk_len.unsigned_abs() as usize;
            if len > MAX_CHUNK_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Chunk size {} exceeds maximum {}", len, MAX_CHUNK_SIZE),
                ));
            }

            // Read chunk data
            self.current_chunk.resize(len, 0);
            self.reader.read_exact(&mut self.current_chunk)?;
            self.chunk_pos = 0;
        }

        // Copy data from current chunk to output buffer
        let available = self.current_chunk.len() - self.chunk_pos;
        let to_copy = available.min(buf.len());
        buf[..to_copy].copy_from_slice(&self.current_chunk[self.chunk_pos..self.chunk_pos + to_copy]);
        self.chunk_pos += to_copy;
        Ok(to_copy)
    }
}

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
            match memrchr(b'\n', &data) {
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
    /// Uses ChunkReader for on-demand chunk reading to minimize memory usage.
    fn process_block_zlib<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Use streaming chunk reader instead of loading entire block into memory
        let chunk_reader = ChunkReader::new(reader, self.dio);

        // Stream: decompress → process lines → compress → write chunks
        let mut decoder = ZlibDecoder::new(chunk_reader);
        let mut encoder = ZlibEncoder::new(Vec::with_capacity(OUTPUT_CHUNK_SIZE), Compression::new(6));

        let mut read_buf = vec![0u8; READ_BUF_SIZE];
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

            match memrchr(b'\n', data) {
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
    /// Uses ChunkReader for on-demand chunk reading to minimize memory usage.
    fn process_block_zstd<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        // Use streaming chunk reader instead of loading entire block into memory
        let chunk_reader = ChunkReader::new(reader, self.dio);

        // Stream: decompress → process lines → compress → write chunks
        // Use compression level 1 for speed (was 3)
        let mut decoder = ZstdDecoder::new(chunk_reader)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd decoder init failed: {}", e)))?;

        // Use multithread zstd compression for better performance on large data
        let mut encoder = ZstdEncoder::new(Vec::with_capacity(OUTPUT_CHUNK_SIZE), 1)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd encoder init failed: {}", e)))?;
        // Enable multithreaded compression (0 = auto-detect CPU count)
        encoder.multithread(0)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd multithread init failed: {}", e)))?;

        let mut read_buf = vec![0u8; READ_BUF_SIZE];
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

            match memrchr(b'\n', data) {
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
        use memchr::memchr;
        let mut start = 0;
        while start < data.len() {
            let end = memchr(b'\n', &data[start..])
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
        use memchr::memchr;
        let mut start = 0;
        while start < data.len() {
            let end = memchr(b'\n', &data[start..])
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