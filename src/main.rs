use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::fs::OpenOptions;
use tokio::fs::File;
use tokio::sync::mpsc;
use tokio::io::{AsyncBufReadExt, BufReader};

const NUM_SHARDS: usize = 16;

struct ShardDB {
    shards: Vec<Mutex<HashMap<String, String>>>
}

impl ShardDB {
    fn new() -> ShardDB {
        let mut shards = Vec::with_capacity(NUM_SHARDS);

        for _ in 0..NUM_SHARDS{
            shards.push(Mutex::new(HashMap::new()));
        }
        ShardDB { shards }
    }

    fn get_shard(&self, key: &str) -> &Mutex<HashMap<String,String>>{
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let indx = (hash as usize) % NUM_SHARDS;

        &self.shards[indx]
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

async fn start_logger(mut receiver: mpsc::Receiver<String>){
    let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("dump.aof")
            .await
            .unwrap();

    while let Some(log) = receiver.recv().await {
        let _ = file.write_all(log.as_bytes()).await;
        let _ = file.write_all(b"\n").await;
    }
}

async fn load_data(db: Arc<ShardDB>) {
    let file = match File::open("dump.aof").await {
        Ok(f) => f,
        Err(_) => return,
    };

    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    println!("Loading data from the disk");

    while let Ok(Some(line)) = lines.next_line().await {
        let cmd = Command::from_input(&line);

        match cmd {
            Command:: SET {key, value} => {
                let mut shard = db.get_shard(&key).lock().unwrap();
                shard.insert(key, value);
            }
            _=> {

            }
        }


    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("listening on 127.0.0.1:6379");

    let db = Arc::new(ShardDB::new());

    load_data(db.clone()).await;
    
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        start_logger(rx).await;
    });

    loop{
        let (socket, _) = listener.accept().await.unwrap();

        let db_handle = db.clone();

        let tx_handler = tx.clone();

        tokio::spawn(async move {
            process(socket, db_handle, tx_handler).await;
        });
    }
}

async fn process(mut socket: tokio::net::TcpStream, db: Arc<ShardDB>, tx: mpsc::Sender<String>) {
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
                let mut map = db.get_shard(&key).lock().unwrap();
                map.insert(key.to_string(), value.to_string());
            }

            let entry = format!("SET {} {}", key, value);
            tx.send(entry).await.unwrap();

            socket.write_all(b"+OK\r\n").await.unwrap()
        }
        Command::GET { key }=> {
            let response = {
                let map = db.get_shard(&key).lock().unwrap();
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
            let err = format!("-ERROR {}\r\n", message);    
            socket.write_all(err.as_bytes()).await.unwrap();
        }
    }
    
}
