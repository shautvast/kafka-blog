use kafka::{client::GroupOffsetStorage, consumer::Consumer};

fn main() -> anyhow::Result<()> {
    let schema: serde_avro_fast::Schema = r#"
        {
          "namespace": "example.avro",
          "type" : "record",
          "name" : "Person",
          "doc" : "Ape descendent creature dwelling on planet Earth",
          "aliases": ["Human"],
          "fields" : [
            {
              "name" : "name",
              "type" : "string"
            },
            {
              "name" : "favorite_number",
              "type" : "int"
            },
            {
              "name" : "height",
              "type" : "double"
            }
          ]
        }"#
    .parse()
    .expect("Failed to parse schema");

    let mut consumer = Consumer::from_hosts(vec![
        "localhost:39092".into(),
        "localhost:39093".into(),
        "localhost:39094".into(),
    ])
    .with_topic_partitions("rustonomicon".to_owned(), &[0])
    .with_group("mygroup".into())
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();

    loop {
        let message = consumer.poll()?;

        for ms in message.iter() {
            for m in ms.messages().iter() {
                let person = serde_avro_fast::from_datum_slice::<Person>(&m.value[5..], &schema)
                    .expect("Failed to deserialize");
                println!("{:?}", person.name);
                consumer.consume_message("rustonomicon", ms.partition(), m.offset)?;
            }
            consumer.commit_consumed()?;
        }
    }

    // Ok(())
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Person<'a> {
    name: &'a str,
    favorite_number: u32,
    height: f64,
}
