pub mod blocks;
pub mod header;
pub mod io;
pub mod toc;

use std::io::{BufReader, BufWriter, Read, Write};

use crate::error::Result;
use crate::format::custom::blocks::BlockProcessor;
use crate::format::custom::header::parse_header;
use crate::format::custom::io::DumpIO;
use crate::format::custom::toc::{parse_toc, Section, TocEntry};
use crate::processor::DataProcessor;

/// Handler for PostgreSQL custom format dumps (-Fc).
pub struct CustomHandler {
    processor: DataProcessor,
}

impl CustomHandler {
    pub fn new(processor: DataProcessor) -> Self {
        Self { processor }
    }

    /// Process a custom format dump from reader to writer.
    /// `initial_bytes` contains the bytes already read for format detection.
    pub fn process<R: Read, W: Write>(
        &mut self,
        reader: R,
        writer: W,
        initial_bytes: &[u8],
    ) -> Result<()> {
        let mut reader = BufReader::with_capacity(65536, reader);
        let mut writer = BufWriter::with_capacity(65536, writer);

        // Parse header (bypasses to output)
        let header = parse_header(&mut reader, &mut writer, initial_bytes)?;

        // Parse TOC entries (bypasses to output)
        let entries = parse_toc(&mut reader, &mut writer, &header)?;

        // Extract comments from TOC entries to build mutation map
        self.extract_comments(&entries);

        // Build a map of dump_id -> table info for data blocks
        let data_entries = self.build_data_map(&entries);

        let dio = DumpIO::new(header.int_size, header.offset_size);

        // Process data blocks
        loop {
            let mut block_type = [0u8; 1];
            match reader.read_exact(&mut block_type) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Block type 0x04 = END
            if block_type[0] == 0x04 {
                writer.write_all(&block_type)?;
                writer.flush()?;
                break;
            }

            // Block type 0x01 = DATA
            if block_type[0] == 0x01 {
                let dump_id = dio.read_int(&mut reader)
                    .map_err(|e| {
                        eprintln!("Failed to read dump_id after DATA block");
                        e
                    })?;

                // Check if this dump_id is in our data_entries map
                if let Some(info) = data_entries.get(&dump_id) {
                    // Set up processor for this table
                    if !info.copy_stmt.is_empty() {
                        self.processor.setup_table(&info.copy_stmt);
                    }

                    if self.processor.has_mutations() || self.processor.is_delete() {
                        // Process with mutations
                        writer.write_all(&block_type)?;
                        dio.write_int(&mut writer, dump_id)?;
                        let mut bp = BlockProcessor::new(&dio, header.compression, &mut self.processor);
                        bp.process_block(&mut reader, &mut writer)?;
                    } else {
                        // No mutations: pass through
                        writer.write_all(&block_type)?;
                        dio.write_int(&mut writer, dump_id)?;
                        let bp = BlockProcessor::new(&dio, header.compression, &mut self.processor);
                        bp.pass_through_block(&mut reader, &mut writer)?;
                    }

                    self.processor.reset_table();
                } else {
                    // Entry not in data_entries map - pass through
                    writer.write_all(&block_type)?;
                    dio.write_int(&mut writer, dump_id)?;
                    let bp = BlockProcessor::new(&dio, header.compression, &mut self.processor);
                    bp.pass_through_block(&mut reader, &mut writer)?;
                }
            } else {
                // Other block types (BLOBS, etc.) - pass through
                writer.write_all(&block_type)?;
                // Read and write dump_id for other block types too
                let dump_id = dio.read_int(&mut reader)?;
                dio.write_int(&mut writer, dump_id)?;
                let bp = BlockProcessor::new(&dio, header.compression, &mut self.processor);
                bp.pass_through_block(&mut reader, &mut writer)?;
            }
        }

        writer.flush()?;
        Ok(())
    }

    /// Extract COMMENT ON statements from TOC definitions to build mutation map.
    fn extract_comments(&mut self, entries: &[TocEntry]) {
        for entry in entries {
            if entry.desc == "COMMENT" {
                // Parse the definition for mutation comments
                self.processor.parse_comment(&entry.defn);
            }
        }
    }

    /// Build a map from dump_id to data entry info.
    fn build_data_map(&self, entries: &[TocEntry]) -> std::collections::HashMap<i32, DataEntryInfo> {
        let mut map = std::collections::HashMap::new();
        for entry in entries {
            if entry.section == Section::Data || entry.desc == "TABLE DATA" {
                map.insert(
                    entry.dump_id,
                    DataEntryInfo {
                        copy_stmt: entry.copy_stmt.clone(),
                        _tag: entry.tag.clone(),
                        _namespace: entry.namespace.clone(),
                    },
                );
            }
        }
        map
    }
}

struct DataEntryInfo {
    copy_stmt: String,
    _tag: String,
    _namespace: String,
}