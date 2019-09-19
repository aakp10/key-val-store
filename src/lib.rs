use std::collections::HashMap;
use std::path::{Path};
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom, BufWriter};
use std::fs::{File, OpenOptions};
use std::io;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{crc32};

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

    pub fn load(&mut self) -> io::Result<()> {
        let mut file_buffer = BufReader::new(&mut self.file);
        loop {
            // TODO: https://stackoverflow.com/questions/34878970/how-to-get-current-cursor-position-in-file/
            let current_pos = file_buffer.seek(SeekFrom::Current(0))?;
            //get the key value pair stored at this location
            let current_kv_pair = process_record(&mut file_buffer);
            let kv = match current_kv_pair {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        },
                        _ => return Err(err),
                    }
                },
            };
 
            self.index.insert(kv.key, current_pos);
        }
        Ok(())
    }

    fn seek_to_end(&mut self) {
        self.file.seek(SeekFrom::End(0)).expect("Error changing the current position by the sepicified offset");
    }

    fn get_to_position(&mut self, position: u64) {
        self.file.seek(SeekFrom::Start(position));
    }

    pub fn insert(&mut self, key: &ByteStr, val: &ByteStr) -> io::Result<()> {
        //bufReader of the file
        self.seek_to_end();
        let mut file_buffer = BufWriter::new(&mut self.file);

        //convert to the req storage format
        let key_len = key.len();
        let val_len = val.len();
        let data_len = (key_len + val_len) as usize;
        let mut data_buffer = ByteString::with_capacity(data_len);
        //insert key and value into this data_buffer
        for &byte in key {
            data_buffer.push(byte);
        }
        for &byte in val {
            data_buffer.push(byte);
        }
        //calculate checksum
        let checksum = crc32::checksum_ieee(&data_buffer);
        let cur_position = file_buffer.seek(SeekFrom::Current(0)).expect("Error changing the current position by the sepicified offset");
        file_buffer.write_u32::<LittleEndian>(checksum).expect("Error writing to buffer");
        file_buffer.write_u32::<LittleEndian>(key_len as u32).expect("Error writing to buffer");
        file_buffer.write_u32::<LittleEndian>(val_len as u32).expect("Error writing to buffer");
        file_buffer.write_all(&data_buffer[..]).expect("Error writing to buffer");
        
        self.index.insert(key.to_vec(), cur_position);
        Ok(())
    }

    pub fn update(&mut self, key: &ByteStr, val: &ByteStr) -> io::Result<()> {
        self.insert(key, val)
    }

    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        //fetch the position from the kv hashmap
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };
        //seek the file ref to `position`
        self.get_to_position(position);
        //process the corresonding record
        let mut file_buffer = BufReader::new(&mut self.file);
        let kv_pair = process_record(&mut file_buffer)?;
        Ok(Some(ByteString::from(kv_pair.value)))
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