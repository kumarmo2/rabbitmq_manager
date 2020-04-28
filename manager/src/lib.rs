use lapin::{
    options::BasicPublishOptions, BasicProperties, Channel, CloseOnDrop, ConnectionProperties,
};
use r2d2::{ManageConnection, Pool};
use serde::Serialize;
use serde_json::to_vec;
use smol::block_on;
use std::time::Duration;

pub struct RabbitMqManager {
    channel_pool: Pool<ChannelManager>,
    //After initial somewhat stabilization of the library, remove tokio, and use a light weight library.
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
}

// Make sure queue exists before calling this method.
impl RabbitMqManager {
    pub fn publish_message_to_queue_sync<T: Serialize>(
        &self,
        queue_name: &str,
        payload: &T,
    ) -> Result<(), lapin::Error> {
        let payload_vec = to_vec(payload).unwrap();
        let result = block_on(self.channel_pool.clone().get().unwrap().basic_publish(
            "",
            queue_name,
            BasicPublishOptions::default(),
            payload_vec,
            BasicProperties::default(),
        ));
        match result {
            Ok(_) => {
                return Ok(());
            }
            Err(reason) => {
                // TODO: this looks awkard. Should be a better syntax.
                return Err(reason);
            }
        }
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

struct ChannelManager {
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
        return block_on(self.connection_pool.clone().get().unwrap().create_channel());
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
