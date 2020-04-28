use manager::RabbitMqManager;
use serde::{Deserialize, Serialize};
use std::time::Duration;

//TODO: in the library, remove most of the calls to unwrap.

#[derive(Debug, Serialize, Deserialize)]
struct MyStruct {
    name: String,
}

fn main() {
    let addr = "amqp://guest:guest@127.0.0.1:5672/%2f";
    let x = MyStruct {
        name: "kumarmo2".to_string(),
    };
    let manager = RabbitMqManager::new(addr);
    // for count in 0..100 {
    match manager.publish_message_to_queue_sync("hello_three", &x) {
        // Ok(_) => println!("published {} message", count),
        Ok(_) => println!("published message"),
        Err(reason) => {
            println!("could not publish the message: {}", reason);
        }
    }
    // }
    // std::thread::sleep(Duration::from_secs(100));
}
