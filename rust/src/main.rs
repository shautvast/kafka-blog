use apache_avro::types::Value;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, Message,
};
use schema_registry_converter::async_impl::{avro::AvroDecoder, schema_registry::SrSettings};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "mygroup")
        .set(
            "bootstrap.servers",
            "localhost:39092,localhost:39093,localhost:39094",
        )
        .create()?;
    let avro_decoder = AvroDecoder::new(SrSettings::new("http://localhost:8081".into()));

    consumer.subscribe(&["rustonomicon"])?;

    while let Ok(message) = consumer.recv().await {
        let value_result = avro_decoder.decode(message.payload()).await?.value;
        if let Value::Record(value_result) = value_result {
            println!("{:?}", value_result.get(0));
        }

        consumer.commit_message(&message, CommitMode::Async)?;
    }

    Ok(())
}
