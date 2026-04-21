use std::io::{BufRead, BufReader, BufWriter, Read, Write};

use crate::error::Result;
use crate::processor::DataProcessor;

/// Handler for PostgreSQL plain text dump format (-Fp).
pub struct PlainHandler {
    processor: DataProcessor,
}

impl PlainHandler {
    pub fn new(processor: DataProcessor) -> Self {
        Self { processor }
    }

    /// Process a plain format dump from reader to writer.
    /// If `initial_bytes` is provided, those bytes are prepended to the stream.
    pub fn process<R: Read, W: Write>(
        &mut self,
        reader: R,
        writer: W,
        initial_bytes: &[u8],
    ) -> Result<()> {
        let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, writer);
        let mut is_data = false;
        let mut comment_buf: Option<String> = None;

        let combined = std::io::Cursor::new(initial_bytes.to_vec()).chain(reader);
        let buf_reader = BufReader::with_capacity(2 * 1024 * 1024, combined);

        for line_result in buf_reader.lines() {
            let line = line_result?;

            if is_data {
                if line == "\\." {
                    if !self.processor.is_delete() {
                        writer.write_all(b"\\.\n")?;
                    }
                    is_data = false;
                    self.processor.reset_table();
                    continue;
                }

                if let Some(mutated) = self.processor.process_line(line.as_bytes()) {
                    writer.write_all(mutated)?;
                    writer.write_all(b"\n")?;
                }
                continue;
            }

            if let Some(ref mut buf) = comment_buf {
                buf.push('\n');
                buf.push_str(&line);
                if line.ends_with("';") {
                    let full_comment = std::mem::take(buf);
                    comment_buf = None;
                    self.processor.parse_comment(&full_comment);
                    writer.write_all(full_comment.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
                continue;
            }

            if (line.starts_with("COMMENT ON COLUMN ") || line.starts_with("COMMENT ON TABLE "))
                && line.contains("'anon: ")
                && !line.ends_with("';")
            {
                comment_buf = Some(line);
                continue;
            }

            self.processor.parse_comment(&line);

            if self.processor.setup_table(&line) {
                if !self.processor.is_delete() {
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
                is_data = true;
                continue;
            }

            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;
        self.processor.emit_summary();
        Ok(())
    }
}
