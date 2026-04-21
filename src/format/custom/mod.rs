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
use crate::FastMap;

/// Handler for PostgreSQL custom format dumps (-Fc).
pub struct CustomHandler {
    processor: DataProcessor,
    verbose: bool,
    zstd_level: i32,
    zstd_threads: u32,
}

impl CustomHandler {
    pub fn new(processor: DataProcessor) -> Self {
        Self {
            processor,
            verbose: false,
            zstd_level: 1,
            zstd_threads: 0,
        }
    }

    pub fn verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn zstd_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    pub fn zstd_threads(mut self, threads: u32) -> Self {
        self.zstd_threads = threads;
        self
    }

    pub fn process<R: Read, W: Write>(
        &mut self,
        reader: R,
        writer: W,
        initial_bytes: &[u8],
    ) -> Result<()> {
        let mut reader = BufReader::with_capacity(2 * 1024 * 1024, reader);
        let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, writer);

        let header = parse_header(&mut reader, &mut writer, initial_bytes, self.verbose)?;
        let entries = parse_toc(&mut reader, &mut writer, &header, self.verbose)?;

        self.extract_comments(&entries);
        let data_entries = self.build_data_map(&entries);
        let dio = DumpIO::new(header.int_size, header.offset_size);

        loop {
            let mut block_type = [0u8; 1];
            match reader.read_exact(&mut block_type) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            if block_type[0] == 0x04 {
                writer.write_all(&block_type)?;
                writer.flush()?;
                break;
            }

            if block_type[0] == 0x01 {
                let dump_id = dio.read_int(&mut reader)?;
                if let Some(info) = data_entries.get(&dump_id) {
                    if !info.copy_stmt.is_empty() {
                        self.processor.setup_table(&info.copy_stmt);
                    }
                    if self.processor.has_mutations() || self.processor.is_delete() {
                        writer.write_all(&block_type)?;
                        dio.write_int(&mut writer, dump_id)?;
                        let mut bp = BlockProcessor::new(
                            &dio,
                            header.compression,
                            &mut self.processor,
                            self.zstd_level,
                            self.zstd_threads,
                        );
                        bp.process_block(&mut reader, &mut writer)?;
                    } else {
                        writer.write_all(&block_type)?;
                        dio.write_int(&mut writer, dump_id)?;
                        let bp = BlockProcessor::new(
                            &dio,
                            header.compression,
                            &mut self.processor,
                            self.zstd_level,
                            self.zstd_threads,
                        );
                        bp.pass_through_block(&mut reader, &mut writer)?;
                    }
                    self.processor.reset_table();
                } else {
                    writer.write_all(&block_type)?;
                    dio.write_int(&mut writer, dump_id)?;
                    let bp = BlockProcessor::new(
                        &dio,
                        header.compression,
                        &mut self.processor,
                        self.zstd_level,
                        self.zstd_threads,
                    );
                    bp.pass_through_block(&mut reader, &mut writer)?;
                }
            } else {
                writer.write_all(&block_type)?;
                let dump_id = dio.read_int(&mut reader)?;
                dio.write_int(&mut writer, dump_id)?;
                let bp = BlockProcessor::new(
                    &dio,
                    header.compression,
                    &mut self.processor,
                    self.zstd_level,
                    self.zstd_threads,
                );
                bp.pass_through_block(&mut reader, &mut writer)?;
            }
        }

        writer.flush()?;
        self.processor.emit_summary();
        Ok(())
    }

    fn extract_comments(&mut self, entries: &[TocEntry]) {
        for entry in entries {
            if entry.desc == "COMMENT" {
                self.processor.parse_comment(&entry.defn);
            }
        }
    }

    fn build_data_map(&self, entries: &[TocEntry]) -> FastMap<i32, DataEntryInfo> {
        let mut map = FastMap::new();
        for entry in entries {
            if entry.section == Section::Data || entry.desc == "TABLE DATA" {
                map.insert(
                    entry.dump_id,
                    DataEntryInfo {
                        copy_stmt: entry.copy_stmt.clone(),
                    },
                );
            }
        }
        map
    }
}

struct DataEntryInfo {
    copy_stmt: String,
}
