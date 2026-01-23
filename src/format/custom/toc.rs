use std::io::{Read, Write};

use crate::error::Result;
use crate::format::custom::header::Header;
use crate::format::custom::io::DumpIO;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    None,
    PreData,
    Data,
    PostData,
}

impl Section {
    pub fn from_i32(val: i32) -> Self {
        match val {
            1 => Section::PreData,
            2 => Section::Data,
            3 => Section::PostData,
            _ => Section::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataState {
    Unknown,
    NeedData,
    NoData,
}

impl DataState {
    pub fn from_i32(val: i32) -> Self {
        match val {
            1 => DataState::NeedData,
            2 => DataState::NoData,
            _ => DataState::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TocEntry {
    pub dump_id: i32,
    pub section: Section,
    pub tag: String,
    pub desc: String,
    pub defn: String,
    pub copy_stmt: String,
    pub drop_stmt: String,
    pub namespace: String,
    pub tablespace: String,
    pub tableam: String,
    pub owner: String,
    pub dependencies: Vec<i32>,
    pub offset: i64,
    pub data_state: DataState,
}

/// Parse all TOC entries from the dump.
/// Reads and bypasses all data to the output writer.
pub fn parse_toc<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    header: &Header,
) -> Result<Vec<TocEntry>> {
    let dio = DumpIO::new(header.int_size, header.offset_size);

    // Read TOC count
    let toc_count = dio.read_int_bypass(reader, writer)?;
    let mut entries = Vec::with_capacity(toc_count.max(0) as usize);

    for _ in 0..toc_count {
        let dump_id = dio.read_int_bypass(reader, writer)?;
        
        // hadDumper (legacy, always present)
        let _had_dumper = dio.read_int_bypass(reader, writer)?;

        // table_oid (first OID string)
        let _table_oid = dio.read_string_bypass(reader, writer)?;
        // oid (second OID string)
        let _oid = dio.read_string_bypass(reader, writer)?;
        // Tag
        let tag = dio.read_string_bypass(reader, writer)?.unwrap_or_default();
        // Desc
        let desc = dio.read_string_bypass(reader, writer)?.unwrap_or_default();

        // Section
        let section_raw = dio.read_int_bypass(reader, writer)?;
        let section = Section::from_i32(section_raw);

        // defn
        let defn = dio.read_string_bypass(reader, writer)?.unwrap_or_default();
        // drop_stmt
        let drop_stmt = dio.read_string_bypass(reader, writer)?.unwrap_or_default();
        // copy_stmt
        let copy_stmt = dio.read_string_bypass(reader, writer)?.unwrap_or_default();
        // namespace
        let namespace = dio.read_string_bypass(reader, writer)?.unwrap_or_default();

        // tablespace
        let tablespace = dio.read_string_bypass(reader, writer)?.unwrap_or_default();

        // tableam (added in format 1.14)
        let tableam = if header.is_version_at_least(1, 14, 0) {
            dio.read_string_bypass(reader, writer)?.unwrap_or_default()
        } else {
            String::new()
        };

        // owner
        let owner = dio.read_string_bypass(reader, writer)?.unwrap_or_default();

        // with_oids (string)
        let _with_oids = dio.read_string_bypass(reader, writer)?;

        // Dependencies
        let mut dependencies = Vec::new();
        loop {
            let dep_str = dio.read_string_bypass(reader, writer)?;
            match dep_str {
                Some(s) if !s.is_empty() => {
                    if let Ok(dep_id) = s.parse::<i32>() {
                        dependencies.push(dep_id);
                    }
                }
                _ => break,
            }
        }

        // data_state (byte, not int!)
        let data_state_byte = DumpIO::read_byte(reader)?;
        writer.write_all(&[data_state_byte])?;
        let data_state = DataState::from_i32(data_state_byte as i32);

        // Offset
        let offset = dio.read_offset_bypass(reader, writer)?;

        entries.push(TocEntry {
            dump_id,
            section,
            tag,
            desc,
            defn,
            copy_stmt,
            drop_stmt,
            namespace,
            tablespace,
            tableam,
            owner,
            dependencies,
            offset,
            data_state,
        });
    }

    Ok(entries)
}
