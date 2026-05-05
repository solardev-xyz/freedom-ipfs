use cid::Cid;
use multihash::Multihash;
use multihash_codetable::{Code, MultihashDigest};
use std::io::Cursor;
use thiserror::Error;

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
}

pub type Result<T> = std::result::Result<T, CoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    cid: Cid,
    data: Vec<u8>,
}

impl Block {
    pub fn new(cid: Cid, data: Vec<u8>) -> Result<Self> {
        verify_block(&cid, &data)?;
        Ok(Self { cid, data })
    }

    pub fn unchecked(cid: Cid, data: Vec<u8>) -> Self {
        Self { cid, data }
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

    pub fn into_parts(self) -> (Cid, Vec<u8>) {
        (self.cid, self.data)
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
    if header_len == 0 || offset + header_len > bytes.len() {
        return Err(CoreError::InvalidCar("invalid header length".into()));
    }
    let header = bytes[offset..offset + header_len].to_vec();
    offset += header_len;

    let mut blocks = Vec::new();
    while offset < bytes.len() {
        let section_len = read_varint(bytes, &mut offset)?;
        if section_len == 0 || offset + section_len > bytes.len() {
            return Err(CoreError::InvalidCar("invalid block section length".into()));
        }

        let section = &bytes[offset..offset + section_len];
        offset += section_len;

        let mut cursor = Cursor::new(section);
        let cid = Cid::read_bytes(&mut cursor)
            .map_err(|err| CoreError::InvalidCar(format!("invalid block cid: {err}")))?;
        let cid_len = cursor.position() as usize;
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

fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<usize> {
    let input = bytes
        .get(*offset..)
        .ok_or_else(|| CoreError::InvalidCar("unexpected end of input".into()))?;
    let before = input.len();
    let (value, rest) = unsigned_varint::decode::usize(input)
        .map_err(|err| CoreError::InvalidCar(format!("invalid varint: {err}")))?;
    *offset += before - rest.len();
    Ok(value)
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
