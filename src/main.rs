use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[tokio::main]

async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("listening on 127.0.0.1:6379");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop{
        let (socket, _) = listener.accept().await.unwrap();

        let db_handle = db.clone();

        tokio::spawn(async move {
            process(socket, db_handle).await;
        });
    }
}

async fn process(mut socket: tokio::net::TcpStream, db: Arc<Mutex<HashMap<String, String>>>) {
    let mut buf = [0; 1024];

    let bytes_read = match socket.read(&mut buf).await {
        Ok(0) => return,
        Ok(n) => n,
        Err(_) => return,
    };

    let input = String::from_utf8_lossy(&buf[0..bytes_read]);

    let parts: Vec<&str> = input.trim().split_whitespace().collect();

    match parts.as_slice(){

        ["SET", key,value] => {
            {
                let mut map = db.lock().unwrap();
                map.insert(key.to_string(), value.to_string());
            }

            socket.write_all(b"+OK\r\n").await.unwrap()
        }
        ["GET", key] => {
            let response = {
                let map = db.lock().unwrap();
                match map.get(*key){
                    Some(value) => Some(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => None,
                }    
            };

            match response {
                Some(value) => socket.write_all(value.as_bytes()).await.unwrap(),
                None => socket.write_all(b"$-1\r\n").await.unwrap(),
            }
        }
        _ => {
            socket.write_all(b"-ERROR Unknown Command\r\n").await.unwrap();
        }
    }
    
}
