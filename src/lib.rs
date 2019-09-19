use std::collections::HashMap;
use std::path::{Path};
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::fs::{File, OpenOptions};
use std::io;
use byteorder::{LittleEndian, ReadBytesExt};
use crc::crc32;

type ByteString = Vec<u8>;
type ByteStr = [u8];
pub struct KeyValuePair {
    key: ByteString,
    value: ByteString,
}

pub struct KVStore {
    file: File,
    pub index: HashMap<ByteString, u64>,
}

impl KVStore {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
                   .read(true)
                   .write(true)
                   .create(true)
                   .append(true)
                   .open(path).expect("Unable to open the file");
        let store_instance = KVStore {
            file,
            index: HashMap::new()
        };
        Ok(store_instance)
    }

    pub fn load(&mut self) {
        let mut file_buffer = BufReader::new(&mut self.file);
        loop {
            // TODO: https://stackoverflow.com/questions/34878970/how-to-get-current-cursor-position-in-file/
            let current_pos = file_buffer.seek(SeekFrom::Current(0));
            //get the key value pair stored at this location
            let current_kv_pair = process_record(&mut file_buffer);
        }
    }

}

pub fn process_record<R: Read>(file_buffer: &mut R) -> io::Result<KeyValuePair>{
        // read crc
        let crc_logged = file_buffer.read_u32::<LittleEndian>()?;
        // read key_len
        let key_len = file_buffer.read_u32::<LittleEndian>()?;
        // read data_len
        let val_len = file_buffer.read_u32::<LittleEndian>()?;
        let data_len = (key_len + val_len) as usize;
        // read key_len + data_len bytes from the BufReader
        let mut buffer = ByteString::with_capacity(data_len);
        //Handle error
        file_buffer.take(data_len as u64)
                   .read_to_end(&mut buffer)?;
        //assert if data_len bytes have been read
        debug_assert_eq!(buffer.len(), data_len);
        //validate crc
        let crc_generated = crc32::checksum_ieee(&buffer);
        if crc_generated != crc_logged {
            panic!{"corrupted data"};
        }
        //split this data into key and value
        let value = buffer.split_off(key_len as usize);
        let key = buffer;
        //return this {key,value} wrapped in a Result instance
        Ok(KeyValuePair{key, value})
    }