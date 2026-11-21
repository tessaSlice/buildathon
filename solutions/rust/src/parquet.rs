//! Uncompressed PLAIN Parquet for the two output tables.
//! `day` is INT32 annotated DATE. Strings are BYTE_ARRAY annotated UTF8.
//! Nullable strings carry definition levels. Integer width is not fixed by the checker.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

const STOP: u8 = 0;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    I64,
    Date,
    Str,
    StrOpt,
}

pub struct Col {
    pub name: String,
    pub kind: Kind,
    pub i32s: Vec<i32>,
    pub i64s: Vec<i64>,
    pub lens: Vec<u32>,
    pub data: Vec<u8>,
    pub present: Vec<u8>,
}

struct Chunk {
    start: i64,
    total: i64,
}

pub fn write_table(path: &Path, n: usize, cols: &[Col]) -> io::Result<()> {
    for col in cols {
        match col.kind {
            Kind::I64 => assert_eq!(col.i64s.len(), n, "{}", col.name),
            Kind::Date => assert_eq!(col.i32s.len(), n, "{}", col.name),
            Kind::Str => assert_eq!(col.lens.len(), n, "{}", col.name),
            Kind::StrOpt => {
                assert_eq!(col.lens.len(), n, "{}", col.name);
                assert_eq!(col.present.len(), n, "{}", col.name);
            }
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = Vec::with_capacity(1 << 20);
    file.extend_from_slice(b"PAR1");
    let mut chunks = Vec::with_capacity(cols.len());
    for col in cols {
        let start = file.len() as i64;
        let payload = column_payload(col, n);
        assert!(payload.len() <= i32::MAX as usize, "parquet page too large");
        let header = page_header(payload.len(), n);
        file.extend_from_slice(&header);
        file.extend_from_slice(&payload);
        let end = file.len() as i64;
        chunks.push(Chunk {
            start,
            total: end - start,
        });
    }
    let footer = file_meta(n, cols, &chunks);
    file.extend_from_slice(&footer);
    file.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let mut out = BufWriter::new(File::create(path)?);
    out.write_all(&file)?;
    out.flush()
}

fn column_payload(col: &Col, n: usize) -> Vec<u8> {
    match col.kind {
        Kind::I64 => {
            let mut p = Vec::with_capacity(n * 8);
            for v in col.i64s.iter().take(n) {
                p.extend_from_slice(&v.to_le_bytes());
            }
            p
        }
        Kind::Date => {
            let mut p = Vec::with_capacity(n * 4);
            for v in col.i32s.iter().take(n) {
                p.extend_from_slice(&v.to_le_bytes());
            }
            p
        }
        Kind::Str => plain_bytes(&col.lens[..n.min(col.lens.len())], &col.data),
        Kind::StrOpt => {
            let present = &col.present[..n.min(col.present.len())];
            let levels = def_levels(present);
            let mut p = Vec::with_capacity(4 + levels.len() + col.data.len());
            p.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            p.extend_from_slice(&levels);
            let mut off = 0usize;
            for i in 0..n.min(col.lens.len()) {
                let len = col.lens[i] as usize;
                if present.get(i).copied().unwrap_or(0) == 1 {
                    p.extend_from_slice(&(len as u32).to_le_bytes());
                    p.extend_from_slice(&col.data[off..off + len]);
                }
                off += len;
            }
            p
        }
    }
}

fn plain_bytes(lens: &[u32], data: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(data.len() + lens.len() * 4);
    let mut off = 0usize;
    for &len in lens {
        let len = len as usize;
        p.extend_from_slice(&(len as u32).to_le_bytes());
        p.extend_from_slice(&data[off..off + len]);
        off += len;
    }
    p
}

/// Max definition level is 1, so the bit width is 1 and is not stored.
/// One bit-packed run, padded up to a multiple of 8 values.
fn def_levels(present: &[u8]) -> Vec<u8> {
    if present.is_empty() {
        return Vec::new();
    }
    let groups = present.len().div_ceil(8);
    let mut out = Vec::with_capacity(8 + groups);
    write_uleb(&mut out, ((groups as u64) << 1) | 1);
    let mut byte = 0u8;
    let mut bit = 0;
    for i in 0..groups * 8 {
        let v = if i < present.len() { present[i] & 1 } else { 0 };
        byte |= v << bit;
        bit += 1;
        if bit == 8 {
            out.push(byte);
            byte = 0;
            bit = 0;
        }
    }
    out
}

fn page_header(payload: usize, n: usize) -> Vec<u8> {
    let mut b = Vec::with_capacity(64);
    let mut last = 0i16;
    field_i32(&mut b, &mut last, 1, 0); // DATA_PAGE
    field_i32(&mut b, &mut last, 2, payload as i32);
    field_i32(&mut b, &mut last, 3, payload as i32);
    field_struct(&mut b, &mut last, 5, |b| {
        let mut last = 0i16;
        field_i32(b, &mut last, 1, n as i32);
        field_i32(b, &mut last, 2, 0); // PLAIN
        field_i32(b, &mut last, 3, 3); // RLE
        field_i32(b, &mut last, 4, 3); // RLE
        b.push(STOP);
    });
    b.push(STOP);
    b
}

fn file_meta(n: usize, cols: &[Col], chunks: &[Chunk]) -> Vec<u8> {
    let mut b = Vec::with_capacity(4096);
    let mut last = 0i16;
    field_i32(&mut b, &mut last, 1, 1);
    field_list_struct(&mut b, &mut last, 2, cols.len() + 1, |b, i| {
        if i == 0 {
            schema_root(b, cols.len());
        } else {
            schema_leaf(b, &cols[i - 1]);
        }
    });
    field_i64(&mut b, &mut last, 3, n as i64);
    field_list_struct(&mut b, &mut last, 4, 1, |b, _| {
        row_group(b, n, cols, chunks);
    });
    field_str(&mut b, &mut last, 6, "shopping");
    b.push(STOP);
    b
}

fn schema_root(b: &mut Vec<u8>, children: usize) {
    let mut last = 0i16;
    field_str(b, &mut last, 4, "schema");
    field_i32(b, &mut last, 5, children as i32);
    b.push(STOP);
}

fn schema_leaf(b: &mut Vec<u8>, col: &Col) {
    let mut last = 0i16;
    let physical = match col.kind {
        Kind::I64 => 2,
        Kind::Date => 1,
        Kind::Str | Kind::StrOpt => 6,
    };
    let rep = if col.kind == Kind::StrOpt { 1 } else { 0 };
    field_i32(b, &mut last, 1, physical);
    field_i32(b, &mut last, 3, rep);
    field_str(b, &mut last, 4, &col.name);
    match col.kind {
        Kind::Date => {
            field_i32(b, &mut last, 6, 6);
            field_struct(b, &mut last, 10, |b| {
                let mut last = 0i16;
                field_struct(b, &mut last, 6, |b| b.push(STOP));
                b.push(STOP);
            });
        }
        Kind::Str | Kind::StrOpt => {
            field_i32(b, &mut last, 6, 0);
            field_struct(b, &mut last, 10, |b| {
                let mut last = 0i16;
                field_struct(b, &mut last, 1, |b| b.push(STOP));
                b.push(STOP);
            });
        }
        Kind::I64 => {}
    }
    b.push(STOP);
}

fn row_group(b: &mut Vec<u8>, n: usize, cols: &[Col], chunks: &[Chunk]) {
    let mut last = 0i16;
    let total: i64 = chunks.iter().map(|c| c.total).sum();
    field_list_struct(b, &mut last, 1, cols.len(), |b, i| {
        column_chunk(b, &cols[i], &chunks[i], n);
    });
    field_i64(b, &mut last, 2, total);
    field_i64(b, &mut last, 3, n as i64);
    b.push(STOP);
}

fn column_chunk(b: &mut Vec<u8>, col: &Col, chunk: &Chunk, n: usize) {
    let mut last = 0i16;
    field_i64(b, &mut last, 2, chunk.start);
    field_struct(b, &mut last, 3, |b| column_meta(b, col, chunk, n));
    b.push(STOP);
}

fn column_meta(b: &mut Vec<u8>, col: &Col, chunk: &Chunk, n: usize) {
    let mut last = 0i16;
    let physical = match col.kind {
        Kind::I64 => 2,
        Kind::Date => 1,
        Kind::Str | Kind::StrOpt => 6,
    };
    field_i32(b, &mut last, 1, physical);
    let encodings: &[i32] = if col.kind == Kind::StrOpt { &[0, 3] } else { &[0] };
    field_list_i32(b, &mut last, 2, encodings);
    field_list_one_str(b, &mut last, 3, &col.name);
    field_i32(b, &mut last, 4, 0); // UNCOMPRESSED
    field_i64(b, &mut last, 5, n as i64);
    field_i64(b, &mut last, 6, chunk.total);
    field_i64(b, &mut last, 7, chunk.total);
    field_i64(b, &mut last, 9, chunk.start);
    b.push(STOP);
}

fn field_list_one_str(b: &mut Vec<u8>, last: &mut i16, id: i16, s: &str) {
    field_begin(b, last, id, 9);
    list_begin(b, 1, 8);
    write_uleb(b, s.len() as u64);
    b.extend_from_slice(s.as_bytes());
}

fn field_list_i32(b: &mut Vec<u8>, last: &mut i16, id: i16, values: &[i32]) {
    field_begin(b, last, id, 9);
    list_begin(b, values.len(), 5);
    for &v in values {
        write_zigzag_i32(b, v);
    }
}

fn field_list_struct(b: &mut Vec<u8>, last: &mut i16, id: i16, len: usize, mut each: impl FnMut(&mut Vec<u8>, usize)) {
    field_begin(b, last, id, 9);
    list_begin(b, len, 12);
    for i in 0..len {
        each(b, i);
    }
}

fn field_struct(b: &mut Vec<u8>, last: &mut i16, id: i16, body: impl FnOnce(&mut Vec<u8>)) {
    field_begin(b, last, id, 12);
    body(b);
}

fn field_i32(b: &mut Vec<u8>, last: &mut i16, id: i16, v: i32) {
    field_begin(b, last, id, 5);
    write_zigzag_i32(b, v);
}

fn field_i64(b: &mut Vec<u8>, last: &mut i16, id: i16, v: i64) {
    field_begin(b, last, id, 6);
    write_zigzag_i64(b, v);
}

fn field_str(b: &mut Vec<u8>, last: &mut i16, id: i16, s: &str) {
    field_begin(b, last, id, 8);
    write_uleb(b, s.len() as u64);
    b.extend_from_slice(s.as_bytes());
}

fn field_begin(b: &mut Vec<u8>, last: &mut i16, id: i16, ty: u8) {
    let delta = id - *last;
    if delta > 0 && delta <= 15 {
        b.push(((delta as u8) << 4) | ty);
    } else {
        b.push(ty);
        write_zigzag_i32(b, id as i32);
    }
    *last = id;
}

fn list_begin(b: &mut Vec<u8>, len: usize, elem: u8) {
    if len < 15 {
        b.push(((len as u8) << 4) | elem);
    } else {
        b.push(0xF0 | elem);
        write_uleb(b, len as u64);
    }
}

fn write_zigzag_i32(b: &mut Vec<u8>, n: i32) {
    let z = ((n << 1) ^ (n >> 31)) as u32 as u64;
    write_uleb(b, z);
}

fn write_zigzag_i64(b: &mut Vec<u8>, n: i64) {
    let z = ((n << 1) ^ (n >> 63)) as u64;
    write_uleb(b, z);
}

fn write_uleb(b: &mut Vec<u8>, mut n: u64) {
    while n > 127 {
        b.push((n as u8) & 0x7f | 0x80);
        n >>= 7;
    }
    b.push(n as u8);
}
