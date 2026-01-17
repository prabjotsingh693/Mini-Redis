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

enum Command {
    SET { key: String, value: String},
    GET { key: String},
    Invalid { message: String}
}

impl Command {
    fn from_input(input: &str) -> Command{
        let mut parts = input.trim().split_whitespace();
        
        let command_word = match parts.next() {
            Some(w) => w,
            None => return Command::Invalid { message: "Empty command".to_string() },
        };

        let key = match parts.next() {
            Some(w) => w,
            None => return Command::Invalid { message: "Key expected".to_string() },
        };

        match command_word {
            "SET" => {
                let value = parts.collect::<Vec<&str>>().join(" ");
                
                if value.is_empty() {
                    return Command::Invalid { message: "Value expected".to_string() }
                }

                Command::SET { 
                    key: key.to_string(), 
                    value 
                }
            }

            "GET" => Command::GET { 
                key: key.to_string() 
            },
            _ => Command::Invalid { 
                message: "Unknown Command".to_string() 
            },
        }


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

    let command = Command::from_input(&input);

    match command{

        Command::SET { key, value } => {
            {
                let mut map = db.lock().unwrap();
                map.insert(key.to_string(), value.to_string());
            }

            socket.write_all(b"+OK\r\n").await.unwrap()
        }
        Command::GET { key }=> {
            let response = {
                let map = db.lock().unwrap();
                match map.get(&key){
                    Some(value) => Some(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => None,
                }    
            };

            match response {
                Some(value) => socket.write_all(value.as_bytes()).await.unwrap(),
                None => socket.write_all(b"$-1\r\n").await.unwrap(),
            }
        }
        Command::Invalid { message }=> {
            socket.write_all(b"-ERROR Unknown Command\r\n").await.unwrap();
        }
    }
    
}
