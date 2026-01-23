use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::{self, Write}; 

#[tokio::main]
async fn main() {
    // 1. Connect to the Server
    let mut stream = match TcpStream::connect("127.0.0.1:6379").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to my-redis: {}", e);
            return;
        }
    };

    println!("Connected to my-redis at 127.0.0.1:6379");

    // 2. The Interactive Loop (REPL)
    loop {
        
        print!("my-redis> ");
        io::stdout().flush().unwrap();

        // Read user input from keyboard
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let command = input.trim();
        if command.is_empty() {
            continue;
        }
        if command.eq_ignore_ascii_case("exit") {
            break;
        }

        // 3. Send Command to Server
        if let Err(e) = stream.write_all(command.as_bytes()).await {
            eprintln!("Error sending command: {}", e);
            break;
        }

        // 4. Read Response
        let mut buf = [0; 1024];
        let n = match stream.read(&mut buf).await {
            Ok(0) => {
                println!("Server closed connection.");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                eprintln!("Error reading response: {}", e);
                break;
            }
        };

        let response = String::from_utf8_lossy(&buf[0..n]);
        println!("{}", response.trim());
    }
}