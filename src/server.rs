use async_std::{channel, io::{BufRead, BufReader, ReadExt, Write}};
use async_std::prelude::*;
use async_std::{
    channel::Sender,
    net::{SocketAddr, TcpStream},
};
use raft::eraftpb::Message;

use crate::raft::Node;
use std::io::Result;

const TYPE_ACCEPT: u8 = 0;
const TYPE_RAFT_LOG: u8 = 1;
pub struct Server { 
}

impl Server {
    pub fn new() -> Self {
        let node = Node::create_raft_follower(my_mailbox, mailboxes, name)
        Server { 
        }
    }
    pub async fn listen(&mut self) -> Result<()> {
        let listener = async_std::net::TcpListener::bind("0.0.0.0:3333").await?;
        loop {
            let (stream, remote_addr) = listener.accept().await?;
            let (tx, rx) = async_std::channel::bounded::<Vec<u8>>(1);
            async_std::task::spawn(handle(tx, stream, remote_addr));
        }
    }
}

pub async fn handle(sender: Sender<Vec<u8>>, stream: TcpStream, addr: SocketAddr) {
    loop {
        let (reader, writer) = &mut (&stream, &stream);
        let mut buf = [0u8; 9];
        let n = reader.read(&mut buf).await;
        let mut lenght_buf = [0u8; 8];
        for (i, b) in buf.iter().enumerate() {
            if i == 0 {
                continue;
            }
            lenght_buf[i - 1] = buf[i];
        }
        let length = usize::from_be_bytes(lenght_buf);
        match buf[0] {
            TYPE_ACCEPT => {
                let (tx,rx ) = channel::bounded::<Message>(1);
                let nd = Node::create_raft_follower(rx, mailboxes, addr.to_string());
                
            }
            TYPE_RAFT_LOG => {
                let mut buf = vec![0u8; length];
                let n = reader.read(&mut buf);
                sender.send(buf).await;
            }
            _ => {
                return;
            }
        }
    }
}
