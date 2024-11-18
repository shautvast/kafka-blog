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
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092,localhost:39093,localhost:39094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8081");

        Person person = new Person();
        person.setName("Arthur Dent");
        person.setFavoriteNumber(42);
        person.setHeight(1.90);

        ProducerRecord<String, Person> record = new ProducerRecord<>("rustonomicon", "arthur", person);

        try (KafkaProducer<String, Person> producer = new KafkaProducer<>(properties)) {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("Message sent to partition " + metadata.partition() + ", offset " + metadata.offset());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}