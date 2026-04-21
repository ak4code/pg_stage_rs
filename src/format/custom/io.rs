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

    pub fn read_byte<R: Read>(reader: &mut R) -> Result<u8> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a signed integer: 1 sign byte + int_size little-endian magnitude bytes.
    pub fn read_int<R: Read>(&self, reader: &mut R) -> Result<i32> {
        self.read_int_inner(reader, None::<&mut std::io::Sink>)
    }

    /// Read an int and also write its raw bytes to the bypass output.
    pub fn read_int_bypass<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<i32> {
        self.read_int_inner(reader, Some(writer))
    }

    /// Debug version — reads, bypasses, and logs raw bytes to stderr.
    pub fn read_int_bypass_debug<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
        label: &str,
    ) -> Result<i32> {
        // Read sign+magnitude onto a stack buffer, bypass to writer, then decode.
        let mut stack = [0u8; 9];
        let total = 1 + self.int_size;
        reader.read_exact(&mut stack[..total])?;
        writer.write_all(&stack[..total])?;
        let value = decode_int(stack[0], &stack[1..1 + self.int_size]);
        eprintln!(
            "[DEBUG] {} raw bytes: sign={:02X} magnitude={:02X?} -> value={}",
            label,
            stack[0],
            &stack[1..1 + self.int_size],
            value
        );
        Ok(value)
    }

    fn read_int_inner<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: Option<&mut W>,
    ) -> Result<i32> {
        let mut stack = [0u8; 9];
        let total = 1 + self.int_size;
        reader.read_exact(&mut stack[..total])?;
        if let Some(w) = writer {
            w.write_all(&stack[..total])?;
        }
        Ok(decode_int(stack[0], &stack[1..1 + self.int_size]))
    }

    /// Write a signed integer as `1 byte sign + int_size bytes`.
    pub fn write_int<W: Write>(&self, writer: &mut W, val: i32) -> Result<()> {
        let (sign, v_abs) = if val < 0 {
            (1u8, val.wrapping_neg())
        } else {
            (0u8, val)
        };
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
        Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
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
        Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
    }

    /// Read an offset as `offset_size` little-endian bytes, in a single read.
    pub fn read_offset<R: Read>(&self, reader: &mut R) -> Result<i64> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf[..self.offset_size])?;
        Ok(decode_offset(&buf[..self.offset_size]))
    }

    /// Read an offset and bypass raw bytes to output.
    pub fn read_offset_bypass<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<i64> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf[..self.offset_size])?;
        writer.write_all(&buf[..self.offset_size])?;
        Ok(decode_offset(&buf[..self.offset_size]))
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

#[inline]
fn decode_int(sign: u8, magnitude: &[u8]) -> i32 {
    let mut value: i32 = 0;
    for (i, &b) in magnitude.iter().enumerate() {
        value |= (b as i32) << (i * 8);
    }
    if sign != 0 { -value } else { value }
}

#[inline]
fn decode_offset(bytes: &[u8]) -> i64 {
    let mut value: i64 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        value |= (b as i64) << (i * 8);
    }
    value
}
