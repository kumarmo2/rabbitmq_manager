use chat_common_types::events::{MessageEvent, MessageEventType};
use manager::RabbitMqManager;
use serde::{Deserialize, Serialize};
use smol;
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
    let event = MessageEvent {
        id: 324,
        user_id: 12,
        event_type: MessageEventType::Send,
    };
    let cpus = num_cpus::get().max(1);
    for _ in 0..cpus {
        std::thread::spawn(move || smol::run(futures::future::pending::<()>()));
    }

    smol::run(async move {
        loop {
            match manager
                .publish_message_to_queue_async("messages", &event)
                .await
            {
                // Ok(_) => println!("published {} message", count),
                Ok(_) => println!("published message"),
                Err(reason) => {
                    println!("could not publish the message: {}", reason);
                }
            }
        }
    });
}
