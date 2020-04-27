#![allow(warnings)]

use lapin::{
    Channel, ChannelStatus, CloseOnDrop, Connection, ConnectionProperties, ConnectionState,
};
use r2d2::{CustomizeConnection, ManageConnection, Pool};
use std::sync::{Arc, Mutex};
use tokio::runtime::{Builder, Runtime};

struct RabbitMqManager {
    channel_pool: Pool<ChannelManager>,
    runtime: Arc<Mutex<Runtime>>,
}

struct ConnectionManager {
    runtime: &'static mut Runtime,
    uri: &'static str,
}

impl ConnectionManager {
    fn new(connection_string: &'static str, runtime: &'static mut Runtime) -> Self {
        ConnectionManager {
            runtime,
            uri: connection_string,
        }
    }
}

struct ChannelManager {
    // TODO: need to think how can I split channels among different connections after certain limit.
    // current_num_of_channels: Arc<Mutex<i32>>,
    connection_pool: Pool<ConnectionManager>,
    runtime: &'static mut Runtime,
}

impl ManageConnection for ChannelManager {
    type Error = lapin::Error;
    type Connection = CloseOnDrop<Channel>;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        return self
            .runtime
            .block_on(self.connection_pool.clone().get().unwrap().create_channel());
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
        return self.runtime.block_on(async move {
            return lapin::Connection::connect(self.uri, ConnectionProperties::default()).await;
        });
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
