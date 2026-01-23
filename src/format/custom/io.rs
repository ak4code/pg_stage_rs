use std::io::{Read, Write};

use crate::error::{Result};

/// Binary I/O utilities for PostgreSQL custom dump format.
///
/// This implementation is kept compatible with the existing Python
/// `custom.py` helper used in this project. That code (and therefore this
/// one) represents integers as `1 byte sign + int_size bytes magnitude`,
/// little-endian, which matches how the working Python version parses and
/// writes dumps.
pub struct DumpIO {
    pub int_size: usize,
    pub offset_size: usize,
}

impl DumpIO {
    pub fn new(int_size: usize, offset_size: usize) -> Self {
        Self { int_size, offset_size }
    }

    /// Read a single byte from the reader.
    pub fn read_byte<R: Read>(reader: &mut R) -> Result<u8> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a signed integer encoded as `1 byte sign + int_size bytes`.
    pub fn read_int<R: Read>(&self, reader: &mut R) -> Result<i32> {
        // Sign byte
        let mut sign_buf = [0u8; 1];
        reader.read_exact(&mut sign_buf)?;
        let sign = sign_buf[0];

        // Magnitude bytes (little-endian)
        let mut buf = vec![0u8; self.int_size];
        reader.read_exact(&mut buf)?;

        let mut value: i32 = 0;
        let mut shift = 0;
        for b in buf {
            if b != 0 {
                value += (b as i32) << shift;
            }
            shift += 8;
        }

        if sign != 0 {
            value = -value;
        }

        Ok(value)
    }

    /// Read an int and also write its raw bytes to the bypass output.
    pub fn read_int_bypass<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<i32> {
        // Sign byte
        let mut sign_buf = [0u8; 1];
        reader.read_exact(&mut sign_buf)?;
        writer.write_all(&sign_buf)?;
        let sign = sign_buf[0];

        // Magnitude bytes
        let mut buf = vec![0u8; self.int_size];
        reader.read_exact(&mut buf)?;
        writer.write_all(&buf)?;

        let mut value: i32 = 0;
        let mut shift = 0;
        for b in buf {
            if b != 0 {
                value += (b as i32) << shift;
            }
            shift += 8;
        }

        if sign != 0 {
            value = -value;
        }

        Ok(value)
    }

    /// Write a signed integer as `1 byte sign + int_size bytes`.
    pub fn write_int<W: Write>(&self, writer: &mut W, val: i32) -> Result<()> {
        let mut v = val;
        let sign: u8 = if v < 0 {
            v = -v;
            1
        } else {
            0
        };

        // Sign byte
        writer.write_all(&[sign])?;

        // Magnitude bytes (little-endian)
        for i in 0..self.int_size {
            let byte = ((v >> (i * 8)) & 0xFF) as u8;
            writer.write_all(&[byte])?;
        }

        Ok(())
    }

    /// Read a string: int length + bytes. Returns None for length 0 or -1.
    pub fn read_string<R: Read>(&self, reader: &mut R) -> Result<Option<String>> {
        let len = self.read_int(reader)?;
        if len <= 0 {
            return Ok(None);
        }
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf)?;
        let s = String::from_utf8_lossy(&buf).to_string();
        Ok(Some(s))
    }

    /// Read a string and bypass to output.
    pub fn read_string_bypass<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<Option<String>> {
        let len = self.read_int_bypass(reader, writer)?;
        if len <= 0 {
            return Ok(None);
        }
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf)?;
        writer.write_all(&buf)?;
        let s = String::from_utf8_lossy(&buf).to_string();
        Ok(Some(s))
    }

    /// Read an offset value as raw bytes (no sign prefix), little-endian.
    pub fn read_offset<R: Read>(&self, reader: &mut R) -> Result<i64> {
        let mut offset: i64 = 0;
        for i in 0..self.offset_size {
            let byte = Self::read_byte(reader)? as i64;
            offset |= byte << (i * 8);
        }
        Ok(offset)
    }

    /// Read an offset and bypass raw bytes to output.
    pub fn read_offset_bypass<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<i64> {
        let mut offset: i64 = 0;
        for i in 0..self.offset_size {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            writer.write_all(&buf)?;
            let byte = buf[0] as i64;
            offset |= byte << (i * 8);
        }
        Ok(offset)
    }

    /// Read exactly n bytes.
    pub fn read_exact<R: Read>(reader: &mut R, n: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read n bytes and bypass to output.
    pub fn read_exact_bypass<R: Read, W: Write>(
        reader: &mut R,
        writer: &mut W,
        n: usize,
    ) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        reader.read_exact(&mut buf)?;
        writer.write_all(&buf)?;
        Ok(buf)
    }
}
