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

const OUTPUT_CHUNK_SIZE: usize = 1024 * 1024;
const MAX_CHUNK_SIZE: usize = 50 * 1024 * 1024;
const READ_BUF_SIZE: usize = 2 * 1024 * 1024;
const COALESCE_TARGET: usize = 256 * 1024;

/// Streaming reader that coalesces many small chunks into larger reads.
/// Critical for -Z0 dumps which can have millions of tiny (~100 byte) chunks.
struct ChunkReader<'a, R: Read> {
    reader: &'a mut R,
    dio: &'a DumpIO,
    buffer: Vec<u8>,
    buf_pos: usize,
    done: bool,
}

impl<'a, R: Read> ChunkReader<'a, R> {
    fn new(reader: &'a mut R, dio: &'a DumpIO) -> Self {
        Self {
            reader,
            dio,
            buffer: Vec::with_capacity(COALESCE_TARGET * 2),
            buf_pos: 0,
            done: false,
        }
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        // Compact the buffer in-place: move unread bytes to the front once,
        // rather than calling drain (which does the same thing + bookkeeping).
        if self.buf_pos > 0 {
            let len = self.buffer.len();
            self.buffer.copy_within(self.buf_pos..len, 0);
            self.buffer.truncate(len - self.buf_pos);
            self.buf_pos = 0;
        }

        while self.buffer.len() < COALESCE_TARGET {
            let chunk_len = self
                .dio
                .read_int(self.reader)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            if chunk_len == 0 {
                self.done = true;
                break;
            }
            let len = chunk_len.unsigned_abs() as usize;
            if len > MAX_CHUNK_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Chunk size {} exceeds maximum {}", len, MAX_CHUNK_SIZE),
                ));
            }
            let start = self.buffer.len();
            self.buffer.resize(start + len, 0);
            self.reader.read_exact(&mut self.buffer[start..])?;
        }
        Ok(())
    }
}

impl<R: Read> Read for ChunkReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.buf_pos >= self.buffer.len() {
            if self.done {
                return Ok(0);
            }
            self.fill_buffer()?;
            if self.buffer.is_empty() {
                return Ok(0);
            }
        }
        let available = self.buffer.len() - self.buf_pos;
        let to_copy = available.min(buf.len());
        buf[..to_copy].copy_from_slice(&self.buffer[self.buf_pos..self.buf_pos + to_copy]);
        self.buf_pos += to_copy;
        Ok(to_copy)
    }
}

pub struct BlockProcessor<'a> {
    dio: &'a DumpIO,
    compression: CompressionMethod,
    processor: &'a mut DataProcessor,
    zstd_level: i32,
    zstd_threads: u32,
}

impl<'a> BlockProcessor<'a> {
    pub fn new(
        dio: &'a DumpIO,
        compression: CompressionMethod,
        processor: &'a mut DataProcessor,
        zstd_level: i32,
        zstd_threads: u32,
    ) -> Self {
        Self {
            dio,
            compression,
            processor,
            zstd_level,
            zstd_threads,
        }
    }

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

    pub fn pass_through_block<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        let mut chunk_reader = ChunkReader::new(reader, self.dio);
        let mut read_buf = vec![0u8; READ_BUF_SIZE];
        let mut output_buf: Vec<u8> = Vec::with_capacity(OUTPUT_CHUNK_SIZE * 2);

        loop {
            let n = chunk_reader.read(&mut read_buf)?;
            if n == 0 {
                break;
            }
            output_buf.extend_from_slice(&read_buf[..n]);
            if output_buf.len() >= OUTPUT_CHUNK_SIZE {
                for chunk in output_buf.chunks(OUTPUT_CHUNK_SIZE) {
                    self.dio.write_int(writer, chunk.len() as i32)?;
                    writer.write_all(chunk)?;
                }
                output_buf.clear();
            }
        }

        if !output_buf.is_empty() {
            self.dio.write_int(writer, output_buf.len() as i32)?;
            writer.write_all(&output_buf)?;
        }

        self.dio.write_int(writer, 0)?;
        Ok(())
    }

    fn process_block_uncompressed<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        let mut chunk_reader = ChunkReader::new(reader, self.dio);
        let mut read_buf = vec![0u8; READ_BUF_SIZE];
        // Reused tail buffer: one allocation per block instead of one per chunk.
        let mut line_tail: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut output_buf: Vec<u8> = Vec::with_capacity(OUTPUT_CHUNK_SIZE * 2);

        loop {
            let n = chunk_reader.read(&mut read_buf)?;
            if n == 0 {
                break;
            }
            let data_slice: &[u8] = if line_tail.is_empty() {
                &read_buf[..n]
            } else {
                line_tail.extend_from_slice(&read_buf[..n]);
                line_tail.as_slice()
            };

            match memrchr(b'\n', data_slice) {
                Some(last_nl) => {
                    let complete_len = last_nl + 1;
                    let (complete, tail) = data_slice.split_at(complete_len);
                    let tail = tail.to_vec();
                    process_complete_lines(self.processor, complete, &mut output_buf);
                    line_tail.clear();
                    line_tail.extend_from_slice(&tail);

                    if output_buf.len() >= OUTPUT_CHUNK_SIZE {
                        flush_uncompressed(self.dio, writer, &mut output_buf)?;
                    }
                }
                None => {
                    if line_tail.is_empty() {
                        line_tail.extend_from_slice(&read_buf[..n]);
                    }
                    // else: line_tail already extended above, leave as-is.
                }
            }
        }

        if !line_tail.is_empty() {
            if let Some(mutated) = self.processor.process_line(&line_tail) {
                output_buf.extend_from_slice(mutated);
            }
        }

        if !output_buf.is_empty() {
            flush_uncompressed(self.dio, writer, &mut output_buf)?;
        }

        self.dio.write_int(writer, 0)?;
        Ok(())
    }

    fn process_block_zlib<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        let chunk_reader = ChunkReader::new(reader, self.dio);
        let mut decoder = ZlibDecoder::new(chunk_reader);
        let mut encoder =
            ZlibEncoder::new(Vec::with_capacity(OUTPUT_CHUNK_SIZE), Compression::new(6));

        let mut read_buf = vec![0u8; READ_BUF_SIZE];
        let mut line_tail: Vec<u8> = Vec::with_capacity(64 * 1024);

        loop {
            let n = decoder
                .read(&mut read_buf)
                .map_err(|e| PgStageError::CompressionError(format!("Zlib decompression failed: {}", e)))?;
            if n == 0 {
                break;
            }
            let data_slice: &[u8] = if line_tail.is_empty() {
                &read_buf[..n]
            } else {
                line_tail.extend_from_slice(&read_buf[..n]);
                line_tail.as_slice()
            };
            match memrchr(b'\n', data_slice) {
                Some(last_nl) => {
                    let complete_len = last_nl + 1;
                    let (complete, tail) = data_slice.split_at(complete_len);
                    let tail = tail.to_vec();
                    process_complete_lines_to_writer(self.processor, complete, &mut encoder)?;
                    line_tail.clear();
                    line_tail.extend_from_slice(&tail);
                    flush_encoder_chunks(self.dio, writer, encoder.get_mut())?;
                }
                None => {
                    if line_tail.is_empty() {
                        line_tail.extend_from_slice(&read_buf[..n]);
                    }
                }
            }
        }

        if !line_tail.is_empty() {
            if let Some(mutated) = self.processor.process_line(&line_tail) {
                encoder
                    .write_all(mutated)
                    .map_err(|e| PgStageError::CompressionError(format!("Zlib compression failed: {}", e)))?;
            }
        }

        let remaining = encoder
            .finish()
            .map_err(|e| PgStageError::CompressionError(format!("Zlib compression finish failed: {}", e)))?;
        if !remaining.is_empty() {
            for chunk in remaining.chunks(OUTPUT_CHUNK_SIZE) {
                self.dio.write_int(writer, chunk.len() as i32)?;
                writer.write_all(chunk)?;
            }
        }
        self.dio.write_int(writer, 0)?;
        Ok(())
    }

    fn process_block_zstd<R: Read, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<()> {
        let chunk_reader = ChunkReader::new(reader, self.dio);
        let mut decoder = ZstdDecoder::new(chunk_reader)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd decoder init failed: {}", e)))?;
        let mut encoder = ZstdEncoder::new(Vec::with_capacity(OUTPUT_CHUNK_SIZE), self.zstd_level)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd encoder init failed: {}", e)))?;
        encoder
            .multithread(self.zstd_threads)
            .map_err(|e| PgStageError::CompressionError(format!("Zstd multithread init failed: {}", e)))?;

        let mut read_buf = vec![0u8; READ_BUF_SIZE];
        let mut line_tail: Vec<u8> = Vec::with_capacity(64 * 1024);

        loop {
            let n = decoder
                .read(&mut read_buf)
                .map_err(|e| PgStageError::CompressionError(format!("Zstd decompression failed: {}", e)))?;
            if n == 0 {
                break;
            }
            let data_slice: &[u8] = if line_tail.is_empty() {
                &read_buf[..n]
            } else {
                line_tail.extend_from_slice(&read_buf[..n]);
                line_tail.as_slice()
            };
            match memrchr(b'\n', data_slice) {
                Some(last_nl) => {
                    let complete_len = last_nl + 1;
                    let (complete, tail) = data_slice.split_at(complete_len);
                    let tail = tail.to_vec();
                    process_complete_lines_to_writer(self.processor, complete, &mut encoder)?;
                    line_tail.clear();
                    line_tail.extend_from_slice(&tail);
                    flush_encoder_chunks_zstd(self.dio, writer, encoder.get_mut())?;
                }
                None => {
                    if line_tail.is_empty() {
                        line_tail.extend_from_slice(&read_buf[..n]);
                    }
                }
            }
        }

        if !line_tail.is_empty() {
            if let Some(mutated) = self.processor.process_line(&line_tail) {
                encoder
                    .write_all(mutated)
                    .map_err(|e| PgStageError::CompressionError(format!("Zstd compression failed: {}", e)))?;
            }
        }

        let remaining = encoder
            .finish()
            .map_err(|e| PgStageError::CompressionError(format!("Zstd compression finish failed: {}", e)))?;
        if !remaining.is_empty() {
            for chunk in remaining.chunks(OUTPUT_CHUNK_SIZE) {
                self.dio.write_int(writer, chunk.len() as i32)?;
                writer.write_all(chunk)?;
            }
        }
        self.dio.write_int(writer, 0)?;
        Ok(())
    }
}

fn process_complete_lines(processor: &mut DataProcessor, data: &[u8], output: &mut Vec<u8>) {
    if processor.is_delete() {
        return;
    }
    if !processor.has_mutations() {
        output.extend_from_slice(data);
        return;
    }
    use memchr::memchr;
    let mut start = 0;
    while start < data.len() {
        let end = memchr(b'\n', &data[start..])
            .map(|p| start + p)
            .unwrap_or(data.len());
        let line = &data[start..end];
        if let Some(mutated) = processor.process_line(line) {
            output.extend_from_slice(mutated);
            if end < data.len() {
                output.push(b'\n');
            }
        }
        start = end + 1;
    }
}

fn process_complete_lines_to_writer<W: Write>(
    processor: &mut DataProcessor,
    data: &[u8],
    writer: &mut W,
) -> Result<()> {
    if processor.is_delete() {
        return Ok(());
    }
    if !processor.has_mutations() {
        writer
            .write_all(data)
            .map_err(|e| PgStageError::CompressionError(format!("Write failed: {}", e)))?;
        return Ok(());
    }
    use memchr::memchr;
    let mut start = 0;
    while start < data.len() {
        let end = memchr(b'\n', &data[start..])
            .map(|p| start + p)
            .unwrap_or(data.len());
        let line = &data[start..end];
        if let Some(mutated) = processor.process_line(line) {
            writer
                .write_all(mutated)
                .map_err(|e| PgStageError::CompressionError(format!("Write failed: {}", e)))?;
            if end < data.len() {
                writer
                    .write_all(b"\n")
                    .map_err(|e| PgStageError::CompressionError(format!("Write failed: {}", e)))?;
            }
        }
        start = end + 1;
    }
    Ok(())
}

fn flush_uncompressed<W: Write>(
    dio: &DumpIO,
    writer: &mut W,
    output_buf: &mut Vec<u8>,
) -> Result<()> {
    for chunk in output_buf.chunks(OUTPUT_CHUNK_SIZE) {
        dio.write_int(writer, chunk.len() as i32)?;
        writer.write_all(chunk)?;
    }
    output_buf.clear();
    Ok(())
}

fn flush_encoder_chunks<W: Write>(
    dio: &DumpIO,
    writer: &mut W,
    inner: &mut Vec<u8>,
) -> Result<()> {
    if inner.len() >= OUTPUT_CHUNK_SIZE {
        for chunk in inner.chunks(OUTPUT_CHUNK_SIZE) {
            dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;
        }
        inner.clear();
    }
    Ok(())
}

fn flush_encoder_chunks_zstd<W: Write>(
    dio: &DumpIO,
    writer: &mut W,
    inner: &mut Vec<u8>,
) -> Result<()> {
    if inner.len() >= OUTPUT_CHUNK_SIZE {
        for chunk in inner.chunks(OUTPUT_CHUNK_SIZE) {
            dio.write_int(writer, chunk.len() as i32)?;
            writer.write_all(chunk)?;
        }
        inner.clear();
    }
    Ok(())
}
