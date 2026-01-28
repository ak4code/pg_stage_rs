use std::io::{Read, Write};

use crate::error::Result;

/// Binary I/O utilities for PostgreSQL custom dump format.
///
/// This implementation matches the Python `custom.py` helper:
/// - Integers: 1 byte sign (0=pos, 1=neg) + int_size bytes magnitude (little-endian).
/// - Strings: Integer length + UTF-8 bytes.
/// - Offsets: offset_size bytes (little-endian).
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

        // Magnitude bytes (little-endian) — stack buffer, max 8 bytes
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf[..self.int_size])?;

        let mut value: i32 = 0;
        let mut shift = 0;
        for &b in &buf[..self.int_size] {
            value |= (b as i32) << shift;
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

        // Magnitude bytes — stack buffer
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf[..self.int_size])?;
        writer.write_all(&buf[..self.int_size])?;

        let mut value: i32 = 0;
        let mut shift = 0;
        for &b in &buf[..self.int_size] {
            value |= (b as i32) << shift;
            shift += 8;
        }

        if sign != 0 {
            value = -value;
        }

        Ok(value)
    }

    /// Write a signed integer as `1 byte sign + int_size bytes`.
    pub fn write_int<W: Write>(&self, writer: &mut W, val: i32) -> Result<()> {
        let (sign, v_abs) = if val < 0 {
            (1u8, val.wrapping_neg()) // Use wrapping_neg to handle i32::MIN edge cases safely
        } else {
            (0u8, val)
        };

        // Write sign + magnitude in one syscall (max 9 bytes: 1 sign + 8 magnitude)
        let mut buf = [0u8; 9];
        buf[0] = sign;
        let mut current = v_abs;
        for i in 0..self.int_size {
            buf[1 + i] = (current & 0xFF) as u8;
            current >>= 8;
        }
        writer.write_all(&buf[..1 + self.int_size])?;

        Ok(())
    }

    /// Read a string: int length + bytes. Returns None for length <= 0.
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

    /// Read a string and bypass raw bytes to output.
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