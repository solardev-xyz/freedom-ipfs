use cid::Cid;
use multihash::Multihash;
use multihash_codetable::{Code, MultihashDigest};
use std::io::Cursor;
use thiserror::Error;

pub mod build_info;
pub mod memory;

pub use memory::{
    btree_map_bytes, BudgetError, ChargedBytes, FixedPool, HeldBytes, MemoryBudget, PoolShare,
    Reservation, ReservedVec,
};

pub const CODEC_DAG_PB: u64 = 0x70;
pub const CODEC_RAW: u64 = 0x55;
pub const HASH_IDENTITY: u64 = 0x00;
pub const HASH_SHA2_256: u64 = 0x12;
pub const DEFAULT_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;
const EMPTY_ROOTS_CAR_V1_HEADER: &[u8] = &[
    0xa2, 0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n', 0x01, 0x65, b'r', b'o', b'o', b't', b's',
    0x80,
];

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid cid: {0}")]
    InvalidCid(String),
    #[error("unsupported multihash code {0}")]
    UnsupportedHash(u64),
    #[error("cid hash mismatch for {cid}")]
    HashMismatch { cid: Cid },
    #[error("block is too large: {actual} bytes > {max} bytes")]
    BlockTooLarge { actual: usize, max: usize },
    #[error("invalid car: {0}")]
    InvalidCar(String),
    #[error("storage error: {0}")]
    Storage(String),
    /// Memory for the work could not be reserved: a host maps it to busy.
    #[error("memory budget: {0}")]
    MemoryBudget(#[from] BudgetError),
}

pub type Result<T> = std::result::Result<T, CoreError>;

/// A block: its CID and its bytes. The bytes are [`ChargedBytes`], so every
/// clone shares one allocation and the memory charges that pay for it, which
/// go when the last clone is dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    cid: Cid,
    data: ChargedBytes,
}

impl Block {
    pub fn new(cid: Cid, data: Vec<u8>) -> Result<Self> {
        verify_block(&cid, &data)?;
        Ok(Self::unchecked(cid, data))
    }

    pub fn unchecked(cid: Cid, data: Vec<u8>) -> Self {
        Self::unchecked_charged(cid, ChargedBytes::from_vec(data))
    }

    /// A block over bytes that may carry memory charges, unverified.
    pub fn unchecked_charged(cid: Cid, data: ChargedBytes) -> Self {
        Self { cid, data }
    }

    /// The block's bytes, with their allocation and charges.
    pub fn charged(&self) -> &ChargedBytes {
        &self.data
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn codec(&self) -> u64 {
        self.cid.codec()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// The CID and a copy of the bytes.
    pub fn into_parts(self) -> (Cid, Vec<u8>) {
        (self.cid, self.data.to_vec())
    }
}

pub trait BlockProvider: Send + Sync {
    fn get_block(&self, cid: &Cid) -> Result<Option<Block>>;

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Option<Vec<u8>>> {
        let Some(block) = self.get_block(cid)? else {
            return Ok(None);
        };
        Ok(Some(block_data_range(block.data(), start, end)))
    }

    fn get_block_ranges(&self, ranges: &[(Cid, u64, u64)]) -> Result<Vec<Option<Vec<u8>>>> {
        ranges
            .iter()
            .map(|(cid, start, end)| self.get_block_range(cid, *start, *end))
            .collect()
    }

    fn retain_block(&self, _cid: &Cid) -> Result<()> {
        Ok(())
    }

    fn release_block(&self, _cid: &Cid) {}
}

pub fn parse_cid(input: &str) -> Result<Cid> {
    input
        .parse::<Cid>()
        .map_err(|err| CoreError::InvalidCid(err.to_string()))
}

pub fn cid_to_string(cid: &Cid) -> String {
    cid.to_string()
}

pub fn cid_from_data(codec: u64, data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(codec, hash)
}

pub fn block_data_range(bytes: &[u8], start: u64, end: u64) -> Vec<u8> {
    if bytes.is_empty() || start > end || start >= bytes.len() as u64 {
        return Vec::new();
    }
    let start = start.min(bytes.len() as u64) as usize;
    let end = end.min(bytes.len() as u64 - 1) as usize;
    bytes[start..=end].to_vec()
}

pub fn verify_block(cid: &Cid, data: &[u8]) -> Result<()> {
    if data.len() > DEFAULT_MAX_BLOCK_SIZE {
        return Err(CoreError::BlockTooLarge {
            actual: data.len(),
            max: DEFAULT_MAX_BLOCK_SIZE,
        });
    }

    let hash = cid.hash();
    match hash.code() {
        HASH_SHA2_256 => {
            let expected = Code::Sha2_256.digest(data);
            if expected.digest() == hash.digest() {
                Ok(())
            } else {
                Err(CoreError::HashMismatch { cid: *cid })
            }
        }
        HASH_IDENTITY => {
            if hash.digest() == data {
                Ok(())
            } else {
                Err(CoreError::HashMismatch { cid: *cid })
            }
        }
        code => Err(CoreError::UnsupportedHash(code)),
    }
}

#[derive(Debug, Clone)]
pub struct CarBlock {
    pub cid: Cid,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CarFile {
    pub header: Vec<u8>,
    pub blocks: Vec<CarBlock>,
}

pub fn parse_car_v1(bytes: &[u8]) -> Result<CarFile> {
    let mut offset = 0;
    let header_len = read_varint(bytes, &mut offset)?;
    let header = take_section(bytes, &mut offset, header_len)
        .ok_or_else(|| CoreError::InvalidCar("invalid header length".into()))?
        .to_vec();

    let mut blocks = Vec::new();
    while offset < bytes.len() {
        let section_len = read_varint(bytes, &mut offset)?;
        let section = take_section(bytes, &mut offset, section_len)
            .ok_or_else(|| CoreError::InvalidCar("invalid block section length".into()))?;

        let mut cursor = Cursor::new(section);
        let cid = Cid::read_bytes(&mut cursor)
            .map_err(|err| CoreError::InvalidCar(format!("invalid block cid: {err}")))?;
        let cid_len = cursor.position() as usize;
        // The CID's own varints (version, codec, the multihash's code and
        // length) are the cid crate's to read, so their encoding is checked
        // here instead: the bytes must be the CID's canonical encoding,
        // which a wrapped or non-minimal varint isn't.
        if section.get(..cid_len) != Some(cid.to_bytes().as_slice()) {
            return Err(CoreError::InvalidCar(
                "block cid isn't canonically encoded".into(),
            ));
        }
        if cid_len > section.len() {
            return Err(CoreError::InvalidCar(
                "block cid consumed beyond section".into(),
            ));
        }
        let data = section[cid_len..].to_vec();
        verify_block(&cid, &data)?;
        blocks.push(CarBlock { cid, data });
    }

    Ok(CarFile { header, blocks })
}

pub fn encode_car_v1(blocks: &[CarBlock]) -> Vec<u8> {
    let mut car = encode_varint(EMPTY_ROOTS_CAR_V1_HEADER.len());
    car.extend_from_slice(EMPTY_ROOTS_CAR_V1_HEADER);

    for block in blocks {
        let mut section = block.cid.to_bytes();
        section.extend_from_slice(&block.data);
        car.extend_from_slice(&encode_varint(section.len()));
        car.extend_from_slice(&section);
    }

    car
}

/// The `len` bytes at `offset`, moving `offset` past them: `None` for an
/// empty section, or one past the end of `bytes`. A length is a varint the
/// file gives, up to `usize::MAX`, so the end is computed without
/// overflowing.
fn take_section<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .filter(|end| len > 0 && *end <= bytes.len())?;
    let section = &bytes[*offset..end];
    *offset = end;
    Some(section)
}

/// A CAR length at `offset`, moving `offset` past it: an unsigned LEB128
/// varint of at most 64 bits, minimally encoded, as a `usize`. Refused:
/// a varint that runs past the end or past ten bytes, a tenth byte with any
/// bit above the 64th set (unsigned-varint 0.8 accepts one, so `2^64 + n`
/// read as `n`), a non-minimal one (a last byte of 0 after others), and a
/// value past `usize::MAX` (on 32-bit targets).
fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<usize> {
    let invalid = |what: &str| CoreError::InvalidCar(format!("invalid varint: {what}"));
    let input = bytes
        .get(*offset..)
        .ok_or_else(|| invalid("unexpected end of input"))?;
    let mut value = 0u64;
    for (index, &byte) in input.iter().enumerate().take(10) {
        if index == 9 && byte > 1 {
            return Err(invalid("past 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            if byte == 0 && index > 0 {
                return Err(invalid("not minimally encoded"));
            }
            *offset += index + 1;
            return usize::try_from(value).map_err(|_| invalid("past this target's usize"));
        }
    }
    Err(invalid(if input.len() < 10 {
        "unexpected end of input"
    } else {
        "past 64 bits"
    }))
}

pub fn encode_varint(value: usize) -> Vec<u8> {
    let mut buf = unsigned_varint::encode::usize_buffer();
    unsigned_varint::encode::usize(value, &mut buf).to_vec()
}

pub fn cid_from_multihash(codec: u64, mh: Multihash<64>) -> Cid {
    Cid::new_v1(codec, mh)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A CAR whose header or section length is a varint near `usize::MAX`:
    /// adding it to the offset overflowed (a debug build panicked there, a
    /// release build wrapped and panicked slicing), and the add-on imports
    /// CAR bytes inside an `extern "C"` function, where a panic aborts.
    #[test]
    fn car_lengths_past_the_end_are_errors_not_panics() {
        let huge = |value: usize| encode_varint(value);
        let header = encode_varint(EMPTY_ROOTS_CAR_V1_HEADER.len())
            .into_iter()
            .chain(EMPTY_ROOTS_CAR_V1_HEADER.iter().copied())
            .collect::<Vec<u8>>();
        for len in [usize::MAX, usize::MAX - 1, usize::MAX / 2 + 1] {
            let mut car = huge(len);
            car.extend_from_slice(b"header");
            assert!(
                matches!(parse_car_v1(&car), Err(CoreError::InvalidCar(_))),
                "header length {len}"
            );
            let mut car = header.clone();
            car.extend(huge(len));
            car.extend_from_slice(b"section");
            assert!(
                matches!(parse_car_v1(&car), Err(CoreError::InvalidCar(_))),
                "section length {len}"
            );
        }
        // Zero-length and just-too-long sections stay errors, and one that
        // ends exactly at the end still parses.
        for car in [
            huge(0),
            [header.clone(), huge(0)].concat(),
            [header.clone(), huge(2), vec![1]].concat(),
        ] {
            assert!(matches!(parse_car_v1(&car), Err(CoreError::InvalidCar(_))));
        }
        let block = CarBlock {
            cid: cid_from_data(CODEC_RAW, b"x"),
            data: b"x".to_vec(),
        };
        assert_eq!(
            parse_car_v1(&encode_car_v1(&[block])).unwrap().blocks.len(),
            1
        );
    }

    /// Review of #69: varints whose value doesn't fit were read wrapped
    /// (unsigned-varint 0.8 takes a tenth byte's high bits, so `2^64 + n`
    /// read as `n`), and the header or a block section was then read as
    /// that small length. Raw encodings, which `encode_varint` can't make.
    #[test]
    fn car_varints_that_dont_fit_or_arent_minimal_are_refused() {
        let data = b"x";
        let cid = cid_from_data(CODEC_RAW, data);
        let section = [cid.to_bytes(), data.to_vec()].concat();
        let empty = encode_car_v1(&[]);
        // `2^64 + n` over ten bytes; `2^32 + n` over five (past u32);
        // `n` padded with a 0x80 and a final 0x00; eleven bytes.
        let wrapped_64 = |n: u8| [vec![0x80 | n], vec![0x80; 8], vec![0x02]].concat();
        let wrapped_32 = |n: u8| vec![0x80 | n, 0x80, 0x80, 0x80, 0x10];
        let padded = |n: u8| vec![0x80 | n, 0x00];
        let eleven = |n: u8| [vec![0x80 | n], vec![0x80; 9], vec![0x00]].concat();
        let header_len = EMPTY_ROOTS_CAR_V1_HEADER.len() as u8;
        for (what, varint) in [
            ("2^64 + n", wrapped_64 as fn(u8) -> Vec<u8>),
            ("non-minimal", padded),
            ("eleven bytes", eleven),
        ] {
            let car = [varint(header_len), EMPTY_ROOTS_CAR_V1_HEADER.to_vec()].concat();
            assert!(
                matches!(parse_car_v1(&car), Err(CoreError::InvalidCar(_))),
                "header, {what}"
            );
            let car = [empty.clone(), varint(section.len() as u8), section.clone()].concat();
            assert!(
                matches!(parse_car_v1(&car), Err(CoreError::InvalidCar(_))),
                "section, {what}"
            );
        }
        // Past u32: too long for the data on 64-bit, past usize on 32-bit.
        let car = [
            empty.clone(),
            wrapped_32(section.len() as u8),
            section.clone(),
        ]
        .concat();
        assert!(matches!(parse_car_v1(&car), Err(CoreError::InvalidCar(_))));
        // The control: the same section, minimally encoded.
        let car = [empty.clone(), vec![section.len() as u8], section.clone()].concat();
        assert_eq!(parse_car_v1(&car).unwrap().blocks.len(), 1);
        // Exactly u64's last byte (0x01) still decodes, and is far past the
        // end.
        let mut max = vec![0xff; 9];
        max.push(0x01);
        assert!(matches!(
            parse_car_v1(&[max, b"x".to_vec()].concat()),
            Err(CoreError::InvalidCar(message)) if message == "invalid header length"
                || message == "invalid varint: past this target's usize"
        ));
    }

    /// The block's CID must be canonically encoded too: its codec varint
    /// written non-minimally, or wrapped past 64 bits, is refused.
    #[test]
    fn car_block_cids_must_be_canonical() {
        let data = b"x";
        let canonical = cid_from_data(CODEC_RAW, data).to_bytes();
        assert_eq!(&canonical[..2], &[0x01, 0x55]);
        let empty = encode_car_v1(&[]);
        let car_with = |cid: Vec<u8>| {
            let section = [cid, data.to_vec()].concat();
            [empty.clone(), encode_varint(section.len()), section].concat()
        };
        assert_eq!(
            parse_car_v1(&car_with(canonical.clone()))
                .unwrap()
                .blocks
                .len(),
            1
        );
        for cid in [
            // The codec 0x55 as `0xd5 0x00`.
            [vec![0x01, 0xd5, 0x00], canonical[2..].to_vec()].concat(),
            // The codec as `2^64 + 0x55`.
            [
                vec![0x01, 0xd5],
                vec![0x80; 8],
                vec![0x02],
                canonical[2..].to_vec(),
            ]
            .concat(),
        ] {
            assert!(
                matches!(
                    parse_car_v1(&car_with(cid.clone())),
                    Err(CoreError::InvalidCar(_))
                ),
                "{cid:02x?}"
            );
        }
    }

    #[test]
    fn cid_round_trips_and_verifies_raw_block() {
        let data = b"hello freedom ipfs";
        let cid = cid_from_data(CODEC_RAW, data);
        assert_eq!(cid.codec(), CODEC_RAW);
        verify_block(&cid, data).unwrap();
        assert!(verify_block(&cid, b"tampered").is_err());
        assert_eq!(parse_cid(&cid.to_string()).unwrap(), cid);
    }

    #[test]
    fn block_data_range_clamps_to_available_bytes() {
        assert_eq!(block_data_range(b"abcdef", 1, 3), b"bcd");
        assert_eq!(block_data_range(b"abcdef", 4, 99), b"ef");
        assert!(block_data_range(b"abcdef", 6, 9).is_empty());
        assert!(block_data_range(b"abcdef", 4, 3).is_empty());
    }

    #[test]
    fn parses_minimal_car() {
        let data = b"car payload";
        let cid = cid_from_data(CODEC_RAW, data);
        let mut section = cid.to_bytes();
        section.extend_from_slice(data);

        let header = data_encoding::HEXLOWER
            .decode(b"a26776657273696f6e0165726f6f747380")
            .unwrap();
        let mut car = encode_varint(header.len());
        car.extend_from_slice(&header);
        car.extend_from_slice(&encode_varint(section.len()));
        car.extend_from_slice(&section);

        let parsed = parse_car_v1(&car).unwrap();
        assert_eq!(parsed.blocks.len(), 1);
        assert_eq!(parsed.blocks[0].cid, cid);
        assert_eq!(parsed.blocks[0].data, data);
    }

    #[test]
    fn encodes_car_round_trip() {
        let data = b"export payload";
        let cid = cid_from_data(CODEC_RAW, data);
        let car = encode_car_v1(&[CarBlock {
            cid,
            data: data.to_vec(),
        }]);

        let parsed = parse_car_v1(&car).unwrap();
        assert_eq!(parsed.blocks.len(), 1);
        assert_eq!(parsed.blocks[0].cid, cid);
        assert_eq!(parsed.blocks[0].data, data);
    }

    #[test]
    fn encodes_and_parses_empty_raw_block() {
        let data = Vec::new();
        let cid = cid_from_data(CODEC_RAW, &data);
        let car = encode_car_v1(&[CarBlock {
            cid,
            data: data.clone(),
        }]);

        let parsed = parse_car_v1(&car).unwrap();
        assert_eq!(parsed.blocks.len(), 1);
        assert_eq!(parsed.blocks[0].cid, cid);
        assert!(parsed.blocks[0].data.is_empty());
    }
}
