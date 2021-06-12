use async_std::{fs, io::prelude::*, path::Path};
use async_std::{io::SeekFrom, task::block_on};
use fs::File;
use std::{io::Result, usize};

use skiplist::SkipMap;

use std::collections::Bound::{Excluded, Included, Unbounded};

#[derive(Clone, Debug)]
pub struct KeyIndex {
    pub offset: usize,
    pub size: usize,
}

pub struct KvEngine {
    pub name: String,

    fd: File,
    fd_offset: usize,

    ifd: File,
    indexes: SkipMap<Vec<u8>, KeyIndex>,
}

impl KvEngine {
    pub fn new(name: String) -> Self {
        let path = format!("{}.kv", name);
        let index_path = format!("{}.idx", name);
        let path = Path::new(&path);
        let index_path = Path::new(&index_path);
        let fd = block_on(
            async_std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .append(true)
                .open(path),
        )
        .unwrap();
        let ifd = block_on(
            async_std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .append(true)
                .open(index_path),
        )
        .unwrap();

        let mut engine = KvEngine {
            name,
            fd,
            fd_offset: 0,
            ifd,
            indexes: SkipMap::new(),
        };
        block_on(engine.load_index()).unwrap();
        block_on(engine.fd.seek(SeekFrom::End(0))).unwrap();
        return engine;
    }
    async fn load_index(&mut self) -> Result<()> {
        let mut buf = Vec::new();
        let mut n = 1;
        while n > 0 {
            let mut buffer = vec![0u8; 1024];
            n = self.ifd.read(&mut buffer).await?;
            buf.extend(buffer[..n].to_vec())
        }

        let mut offset = 0;
        for line in buf.split(|b| *b == b'\n') {
            let line: Vec<&[u8]> = line.split(|b| *b == b',').collect();
            if line.len() != 2 {
                return Ok(());
            }
            let key = line[0].to_vec();
            let size = String::from_utf8(line[1].to_vec())
                .unwrap()
                .parse::<usize>()
                .unwrap();

            self.indexes.insert(key, KeyIndex { offset, size });
            offset += size;
            self.fd_offset = offset
        }
        Ok(())
    }

    async fn append_index(&mut self, k: Vec<u8>, idx: KeyIndex) -> Result<()> {
        let mut line = Vec::new();
        line.extend(k);
        line.extend(format!(",{}\n", idx.size).as_bytes().to_vec());
        self.ifd.write(&line).await?;
        self.ifd.flush().await?;
        Ok(())
    }
    // header = len(key)+len(val)
    // buf = header + key+val
    pub async fn set(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
        let index = KeyIndex {
            offset: self.fd_offset,
            size: val.len(),
        };
        self.indexes.insert(key.clone(), index.clone());
        self.fd_offset += val.len();
        self.fd.seek(SeekFrom::End(0));
        self.fd.write(&val).await?;
        self.fd.flush().await?;
        self.append_index(key, index).await?;
        Ok(())
    }
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Vec<u8>> {
        if let Some(index) = self.indexes.get(&key) {
            let mut buf = vec![0u8; index.size];
            if index.offset > self.fd_offset - index.offset {
                self.fd
                    .seek(SeekFrom::End(-((self.fd_offset - index.offset) as i64)))
                    .await?;
            } else {
                self.fd.seek(SeekFrom::Start((index.offset) as u64)).await?;
            }

            self.fd.read(&mut buf).await?;
            return Ok(buf);
        }
        Ok(vec![0u8; 0])
    }
    pub async fn scan(
        &mut self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
        let mut max = Included(&end_key);
        let mut min = Included(&start_key);
        if end_key.len() == 0 {
            max = Unbounded;
        }
        if start_key.len() == 0 {
            min = Unbounded;
        }
        let iter = self.indexes.range(min, max);
        let mut keys = Vec::with_capacity(1);
        let mut vals = Vec::with_capacity(1);
        let mut i = 0;
        for (k, index) in iter {
            let mut buf = vec![0u8; index.size];
            if i == 0 {
                if (self.fd_offset - index.offset) < index.offset {
                    self.fd
                        .seek(SeekFrom::End(-((self.fd_offset - index.offset) as i64)))
                        .await?;
                } else {
                    self.fd.seek(SeekFrom::Start(index.offset as u64)).await?;
                }
            }
            self.fd.read(&mut buf).await?;
            keys.push(k.to_vec());
            vals.push(buf);
            i += 1;
        }
        Ok((keys, vals))
    }

    pub async fn next(&mut self, key: Vec<u8>) -> Result<(String, Vec<u8>)> {
        for (k, index) in self.indexes.range(Excluded(&key), Unbounded) {
            let mut buf = vec![0u8; index.size];
            if index.offset > self.fd_offset - index.offset {
                self.fd
                    .seek(SeekFrom::End(-((self.fd_offset - index.offset) as i64)))
                    .await?;
            } else {
                self.fd.seek(SeekFrom::Start((index.offset) as u64)).await?;
            }
            self.fd.read(&mut buf).await?;
            return Ok((String::from_utf8(k.to_owned()).unwrap(), buf));
        }
        return Ok(("".to_string(), vec![0]));
    }
}
