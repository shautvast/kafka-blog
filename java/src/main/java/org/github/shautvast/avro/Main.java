package org.github.shautvast.avro;

import example.avro.Person;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8081");

        Person person = new Person();
        person.setName("Arthur Dent");
        person.setFavoriteNumber(42);
        person.setHeight(1.90);

        // Prepare a message to send
        ProducerRecord<String, Person> record = new ProducerRecord<>("rustonomicon", "arthur", person);

        try (KafkaProducer<String, Person> producer = new KafkaProducer<>(properties)) {
            // Send the message; note that this is asynchronous
            Future<RecordMetadata> future = producer.send(record);

            // For this example, we're going to block and get the response; don't do this in production!
            RecordMetadata metadata = future.get();

            // Print the offset and partition of the message
            System.out.println("Message sent to partition " + metadata.partition() + ", offset " + metadata.offset());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}