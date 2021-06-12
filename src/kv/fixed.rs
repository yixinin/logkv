use std::{
    collections::HashMap,
    io::{Result, SeekFrom},
    usize,
};

use async_std::{fs::File, io::prelude::*, path::Path};

use super::KeyIndex;

pub struct FixedKvEngine {
    pub name: String,
    fd: File,
    fd_offset: usize,

    indexes: HashMap<Vec<u8>, KeyIndex>,
    ifd: File,
}

impl FixedKvEngine {
    pub async fn new(name: String) -> Self {
        let path = format!("{}.kv", name);
        let index_path = format!("{}.idx", name);
        let path = Path::new(&path);
        let index_path = Path::new(&index_path);
        let fd = async_std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .await
            .unwrap();
        let ifd = async_std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            // .append(true)
            .open(index_path)
            .await
            .unwrap();
        let mut engine = FixedKvEngine {
            name,
            fd,
            ifd,
            indexes: HashMap::new(),
            fd_offset: 0,
        };
        engine.load_index().await.unwrap();
        engine
            .fd
            .seek(SeekFrom::Start(engine.fd_offset as u64))
            .await
            .unwrap();
        return engine;
    }

    pub async fn load_index(&mut self) -> Result<()> {
        let mut buf = Vec::new();
        let mut n = 1;
        while n > 0 {
            let mut buffer = vec![0u8; 1024];
            n = self.ifd.read(&mut buffer).await?;
            buf.extend(buffer[..n].to_vec())
        }

        for line in buf.split(|b| *b == b'\n') {
            let line: Vec<&[u8]> = line.split(|b| *b == b',').collect();
            if line.len() != 3 {
                return Ok(());
            }
            let key = line[0].to_vec();
            let offset = String::from_utf8(line[1].to_vec())
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let size = String::from_utf8(line[2].to_vec())
                .unwrap()
                .parse::<usize>()
                .unwrap();

            self.indexes.insert(key, KeyIndex { offset, size });
            self.fd_offset += size;
        }
        Ok(())
    }

    async fn flush_index(&mut self) -> Result<()> {
        let mut s = Vec::new();
        for (k, idx) in self.indexes.iter() {
            let mut line = Vec::new();
            line.extend(k);
            line.extend(format!(",{}", idx.offset).as_bytes().to_vec());
            line.extend(format!(",{}\n", idx.size).as_bytes().to_vec());
            s.extend(line)
        }
        self.ifd.seek(SeekFrom::Start(0)).await?;
        self.ifd.write(&s).await?;
        self.ifd.set_len(s.len() as u64).await?;
        self.ifd.flush().await?;
        Ok(())
    }

    pub async fn append_index(&mut self, k: Vec<u8>, idx: KeyIndex) -> Result<()> {
        let mut line = Vec::new();
        line.extend(k);
        line.extend(format!(",{}", idx.offset).as_bytes().to_vec());
        line.extend(format!(",{}\n", idx.size).as_bytes().to_vec());
        self.ifd.seek(SeekFrom::End(0)).await?;
        self.ifd.write(&line).await?;
        self.ifd.flush().await?;
        Ok(())
    }

    async fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
        let index = KeyIndex {
            offset: self.fd_offset,
            size: val.len(),
        };
        self.fd.seek(SeekFrom::End(0)).await?;
        self.fd.write(&val).await?;
        self.fd.flush().await?;
        self.append_index(key.clone(), index.clone()).await?;
        self.indexes.insert(key, index);
        self.fd_offset += val.len();
        Ok(())
    }

    pub async fn set(&mut self, key: Vec<u8>, mut val: Vec<u8>) -> Result<()> {
        if let Some(idx) = self.indexes.get(&key) {
            if idx.size < val.len() {
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
            }
            if idx.size > val.len() {
                for _ in val.len()..idx.size {
                    val.push(0);
                }
            }
            if idx.offset > (self.fd_offset - idx.offset) {
                self.fd
                    .seek(SeekFrom::End(-((self.fd_offset - idx.offset) as i64)))
                    .await?;
            } else {
                self.fd.seek(SeekFrom::Start(idx.offset as u64)).await?;
            }
            self.fd.write(&val).await?;
            self.fd.flush().await?;
            return Ok(());
        }
        return self.insert(key, val).await;
    }

    pub async fn del(&mut self, key: Vec<u8>) -> Result<()> {
        if let Some((_, idx)) = self.indexes.remove_entry(&key) {
            let offset = idx.offset + idx.size;
            if offset > (self.fd_offset - offset) {
                self.fd
                    .seek(SeekFrom::End(-((self.fd_offset - offset) as i64)))
                    .await?;
            } else {
                self.fd.seek(SeekFrom::Start(offset as u64)).await?;
            }

            let mut buf = vec![0u8; self.fd_offset - offset];
            self.fd.read(&mut buf).await?;

            if idx.offset > (self.fd_offset - idx.offset) {
                self.fd
                    .seek(SeekFrom::End(-((self.fd_offset - idx.offset) as i64)))
                    .await?;
            } else {
                self.fd.seek(SeekFrom::Start(idx.offset as u64)).await?;
            }
            self.fd_offset = idx.offset + buf.len();
            if self.fd_offset > 0 {
                self.fd.write(&buf).await?;
                self.fd.flush().await?;
            }

            self.fd.set_len((idx.offset + buf.len()) as u64).await?;

            for (_, v) in self.indexes.iter_mut() {
                if v.offset > idx.offset {
                    v.offset -= idx.size;
                }
            }
            self.flush_index().await?;
            return Ok(());
        }
        Ok(())
    }

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Vec<u8>> {
        if let Some(idx) = self.indexes.get(&key) {
            let mut val = vec![0u8; idx.size];
            if idx.offset > (self.fd_offset - idx.offset) {
                self.fd
                    .seek(SeekFrom::End(-((self.fd_offset - idx.offset) as i64)))
                    .await?;
            } else {
                self.fd.seek(SeekFrom::Start(idx.offset as u64)).await?;
            }
            self.fd.read(&mut val).await?;
            return Ok(val);
        }
        Ok(vec![0u8; 0])
    }
}
