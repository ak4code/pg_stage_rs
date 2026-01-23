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
        let mut writer = BufWriter::with_capacity(65536, writer);
        let mut is_data = false;

        // If we have initial bytes, chain them with the reader
        let combined = std::io::Cursor::new(initial_bytes.to_vec()).chain(reader);
        let buf_reader = BufReader::with_capacity(65536, combined);

        for line_result in buf_reader.lines() {
            let line = line_result?;

            if is_data {
                if line == "\\." {
                    // End of COPY data
                    if !self.processor.is_delete() {
                        writer.write_all(b"\\.\n")?;
                    }
                    is_data = false;
                    self.processor.reset_table();
                    continue;
                }

                // Process data line
                if let Some(mutated) = self.processor.process_line(line.as_bytes()) {
                    writer.write_all(&mutated)?;
                    writer.write_all(b"\n")?;
                }
                continue;
            }

            // Try to parse as comment
            self.processor.parse_comment(&line);

            // Try to parse as COPY statement
            if self.processor.setup_table(&line) {
                if !self.processor.is_delete() {
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
                is_data = true;
                continue;
            }

            // Pass through other lines unchanged
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;
        Ok(())
    }
}
