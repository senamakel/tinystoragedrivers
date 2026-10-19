//! The AMQP side of a handle: one connection, one channel with publisher
//! confirms, and the queue topology behind each logical queue.

use std::collections::HashSet;
use std::sync::Mutex;

use lapin::options::{
    BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions,
    QueuePurgeOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use tinystoragedrivers_core::{Result, StorageError};

use crate::envelope::Envelope;
use crate::names::Names;

/// Persistent delivery mode: the broker writes the message to disk.
const PERSISTENT: u8 = 2;
/// `x-max-priority` of every main queue: priorities `0..=9`.
const MAX_PRIORITY_ARG: i32 = 9;

/// A broker failure as a storage error. The URL (and its credentials) never
/// reaches the message.
pub(crate) fn amqp(error: lapin::Error) -> StorageError {
    StorageError::unavailable("rabbitmq operation failed").with_source(error)
}

/// The live channel and the generation it belongs to. Every reopen bumps the
/// generation; a lease from an older one died with its channel.
pub(crate) struct Link {
    url: String,
    connection: Connection,
    channel: Channel,
    generation: u64,
}

impl Link {
    pub(crate) async fn open(url: &str) -> Result<Self> {
        let connection = Connection::connect(url, ConnectionProperties::default())
            .await
            .map_err(amqp)?;
        let channel = confirming(&connection).await?;
        Ok(Self {
            url: url.to_owned(),
            connection,
            channel,
            generation: 1,
        })
    }

    /// The channel, reopened (and the connection too, if it dropped) when it
    /// has closed. Returns whether it was reopened.
    pub(crate) async fn ensure(&mut self) -> Result<bool> {
        if self.channel.status().connected() {
            return Ok(false);
        }
        if !self.connection.status().connected() {
            self.connection = Connection::connect(&self.url, ConnectionProperties::default())
                .await
                .map_err(amqp)?;
        }
        self.channel = confirming(&self.connection).await?;
        self.generation += 1;
        Ok(true)
    }

    pub(crate) fn channel(&self) -> Channel {
        self.channel.clone()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Close the connection, as a crashed process would lose it.
    pub(crate) async fn close(&self) -> Result<()> {
        self.connection
            .close(200, ShortString::from("closing"))
            .await
            .map_err(amqp)
    }
}

async fn confirming(connection: &Connection) -> Result<Channel> {
    let channel = connection.create_channel().await.map_err(amqp)?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(amqp)?;
    Ok(channel)
}

/// Publish `envelope` to `queue` and wait for the broker's confirm.
///
/// # Errors
///
/// `Unavailable` when the broker fails; `Backend` when it refuses the message
/// or the queue does not exist (the publish is mandatory).
pub(crate) async fn publish(
    channel: &Channel,
    queue: &str,
    envelope: &Envelope,
    priority: u8,
) -> Result<()> {
    let properties = BasicProperties::default()
        .with_message_id(ShortString::from(envelope.id.as_str()))
        .with_content_type(ShortString::from("application/json"))
        .with_delivery_mode(PERSISTENT)
        .with_priority(priority);
    let confirm = channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            &envelope.to_bytes()?,
            properties,
        )
        .await
        .map_err(amqp)?
        .await
        .map_err(amqp)?;
    if confirm.is_nack() {
        return Err(StorageError::backend("rabbitmq refused a message"));
    }
    if confirm.take_message().is_some() {
        return Err(StorageError::backend(
            "rabbitmq returned an unroutable message",
        ));
    }
    Ok(())
}

/// One message pulled with `basic.get`, unacknowledged.
pub(crate) async fn get(
    channel: &Channel,
    queue: &str,
) -> Result<Option<lapin::message::Delivery>> {
    let message = channel
        .basic_get(ShortString::from(queue), BasicGetOptions { no_ack: false })
        .await
        .map_err(amqp)?;
    Ok(message.map(|message| message.delivery))
}

fn durable() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    }
}

fn main_arguments() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-max-priority"),
        AMQPValue::LongInt(MAX_PRIORITY_ARG),
    );
    arguments
}

fn delay_arguments(names: &Names, bucket_ms: u64) -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-message-ttl"),
        AMQPValue::LongInt(i32::try_from(bucket_ms).unwrap_or(i32::MAX)),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString("".into()),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(names.main.as_str().into()),
    );
    arguments
}

/// Logical queues whose broker queues this handle has declared.
#[derive(Debug, Default)]
pub(crate) struct Topology(Mutex<HashSet<String>>);

impl Topology {
    /// Declare the main, dead and wait queues of `names` once per handle.
    /// Redeclaring with the same arguments is a no-op on the broker.
    pub(crate) async fn declare(&self, channel: &Channel, names: &Names) -> Result<()> {
        if self
            .0
            .lock()
            .map_err(|_| StorageError::backend("rabbitmq topology lock poisoned"))?
            .contains(&names.main)
        {
            return Ok(());
        }
        let declare = |name: String, arguments: FieldTable| async move {
            channel
                .queue_declare(ShortString::from(name), durable(), arguments)
                .await
                .map_err(amqp)
        };
        declare(names.main.clone(), main_arguments()).await?;
        declare(names.dead.clone(), FieldTable::default()).await?;
        for (bucket, name) in names.delays() {
            declare(name, delay_arguments(names, bucket)).await?;
        }
        self.0
            .lock()
            .map_err(|_| StorageError::backend("rabbitmq topology lock poisoned"))?
            .insert(names.main.clone());
        Ok(())
    }
}

/// Messages ready in `queue`, by passive declare.
pub(crate) async fn count(channel: &Channel, queue: &str) -> Result<u64> {
    let declared = channel
        .queue_declare(
            ShortString::from(queue),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(amqp)?;
    Ok(u64::from(declared.message_count()))
}

/// Drop every ready message of `queue`.
pub(crate) async fn purge(channel: &Channel, queue: &str) -> Result<u64> {
    let purged = channel
        .queue_purge(ShortString::from(queue), QueuePurgeOptions::default())
        .await
        .map_err(amqp)?;
    Ok(u64::from(purged))
}
