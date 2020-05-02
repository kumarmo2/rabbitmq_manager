use lapin::options::BasicPublishOptions;
use lapin::publisher_confirm::PublisherConfirm;
use lapin::PromiseChain;
use lapin::{publisher_confirm::Confirmation, Channel, CloseOnDrop, ConnectionProperties};
use r2d2::ManageConnection;
use serde::Serialize;
use serde_json::to_vec;
use smol::block_on;
use std::time::Duration;

//reexport
pub use lapin::{
    options::BasicAckOptions,
    options::{BasicConsumeOptions, ConfirmSelectOptions},
    types::FieldTable,
    BasicProperties,
};
pub use r2d2::Pool;

#[derive(Clone)] // This clone was required so that we can expose the channel_pool. This clone is not expensive as Pool is defined as: struct Pool<M>(Arc<Shared<M>>)
pub struct RabbitMqManager {
    channel_pool: Pool<ChannelManager>,
}

impl RabbitMqManager {
    pub fn new(uri: &'static str) -> Self {
        let connection_manager = ConnectionManager::new(uri);
        let channel_manager = ChannelManager::new(connection_manager);
        let channel_pool = Pool::builder()
            .max_size(18)
            .min_idle(Some(2))
            .idle_timeout(Some(Duration::from_secs(60 * 5)))
            .build(channel_manager)
            .expect("could not create channel pool");

        RabbitMqManager { channel_pool }
    }

    fn get_result_from_confirmation(confirm: &Confirmation) -> Result<bool, lapin::Error> {
        match confirm {
            Confirmation::Ack(ack) => {
                // TODO: confirm if the below logic is correct. I have assumed, when the message is delivered,
                // None is returned, else on failure, Some(_) is returned.
                if let Some(_) = ack {
                    println!("failed acknowledgement: {:?}", ack);
                    return Ok(false);
                } else {
                    println!("message published confirm: {:?}", ack);
                    return Ok(true);
                }
            }
            Confirmation::Nack(nack) => {
                println!("message not published confirm: {:?}", nack);
                return Ok(false);
            }
            Confirmation::NotRequested => {
                println!("cnfirm ack not even requested");
                return Ok(false);
            }
        }
    }
}

// Make sure queue exists before calling this method.
impl RabbitMqManager {
    pub async fn publish_message_to_queue_async<T: Serialize>(
        &self,
        queue_name: &str,
        payload: &T,
    ) -> Result<bool, lapin::Error> {
        //TODO: Need to handle the cases, when message is not published.
        let payload_vec = to_vec(payload).unwrap();
        let confirm = self
            .send_message_with_confirm(queue_name, payload_vec)
            .await
            .expect("basic-publish")
            .await
            .expect("publish-confirm");
        return RabbitMqManager::get_result_from_confirmation(&confirm);
    }

    pub fn publish_message_to_queue_sync<T: Serialize>(
        &self,
        queue_name: &str,
        payload: &T,
    ) -> Result<bool, lapin::Error> {
        //TODO: Need to handle the cases, when message is not published.
        let payload_vec = to_vec(payload).unwrap();
        let confirm = block_on(async move {
            self.send_message_with_confirm(queue_name, payload_vec)
                .await
                .expect("basic-publish")
                .await
                .expect("publish-confirm")
        });

        RabbitMqManager::get_result_from_confirmation(&confirm)
    }

    fn send_message_with_confirm(
        &self,
        queue_name: &str,
        payload: Vec<u8>,
    ) -> PromiseChain<PublisherConfirm> {
        self.channel_pool.clone().get().unwrap().basic_publish(
            "",
            queue_name,
            //TODO: make this object just once.
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            payload,
            BasicProperties::default().with_priority(42),
        )
    }

    pub fn get_channel_pool(&self) -> Pool<ChannelManager> {
        self.clone().channel_pool
    }
}

struct ConnectionManager {
    uri: &'static str,
}
impl ConnectionManager {
    fn new(connection_string: &'static str) -> Self {
        ConnectionManager {
            uri: connection_string,
        }
    }
}

pub struct ChannelManager {
    // TODO: need to think how can I split channels among different connections after certain limit.
    // current_num_of_channels: Arc<Mutex<i32>>,
    connection_pool: Pool<ConnectionManager>,
}

impl ChannelManager {
    fn new(connection_manager: ConnectionManager) -> Self {
        let connection_pool;
        let result = Pool::builder()
            // TODO: need to make sure a new connection should only be made, when the existing connections
            // have reached the channels per connection quota.
            // it is very much needed for performance reasons.
            .max_size(5) // TODO: make this size configurable.
            .min_idle(Some(1))
            .idle_timeout(Some(Duration::from_secs(60 * 10)))
            .build(connection_manager);

        match result {
            Ok(pool) => {
                connection_pool = pool;
            }
            Err(reason) => {
                panic!("could not make connection pool {}", reason);
            }
        }

        ChannelManager { connection_pool }
    }
}

impl ManageConnection for ChannelManager {
    type Error = lapin::Error;
    type Connection = CloseOnDrop<Channel>;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        return block_on(async move {
            let channel = self
                .connection_pool
                .clone()
                .get()
                .unwrap()
                .create_channel()
                .await
                .expect("could not create channel");
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .expect("could not set confirm options on channel");
            return Ok(channel);
        });
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn.status().is_connected() {
            true => {
                return Ok(());
            }
            // TODO: send correct error status
            _ => return Err(lapin::Error::InvalidAck),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.status().is_connected()
    }
}

impl ManageConnection for ConnectionManager {
    type Error = lapin::Error;
    type Connection = CloseOnDrop<lapin::Connection>;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        return block_on(lapin::Connection::connect(
            self.uri,
            ConnectionProperties::default(),
        ));
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn.status().connected() {
            true => Ok(()),
            // TODO: send correct error.
            false => Err(lapin::Error::InvalidAck),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.status().connected()
    }
}
