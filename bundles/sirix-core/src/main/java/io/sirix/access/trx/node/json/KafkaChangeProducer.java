package io.sirix.access.trx.node.json;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaChangeProducer {
    private final KafkaProducer<String, String> producer;
    final String topic;

    public KafkaChangeProducer(String bootstrapServers, String topic) {
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void sendChange(String key, String value) {
        
        if (producer != null) {
           
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Send the record to the Kafka topic
            producer.send(record);
        } else {
            throw new IllegalStateException("Kafka producer is not initialized.");
        }
    }

    public void close() {
        // Close the Kafka producer
        if (producer != null) {
            producer.close();
        }
    }
}
