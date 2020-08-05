package com.github.dmtobler.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "localhost:9092";

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 10; i++) {
            // Create a Producer record

            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key
            // id_0 -> Partition 1
            // id_1 -> Partition 0
            // id_2 -> Partition 2
            // id_3 -> Partition 0
            // id_4 -> Partition 2
            // id_5 -> Partition 2
            // id_6 -> Partition 0
            // id_7 -> Partition 2
            // id_8 -> Partition 1
            // id_9 -> Partition 2

            // Re-running code shows that the same key always goes to the same partition
            // *** By providing a key, we guarantee that the same key always goes to the same partition ***
            // NOTE: The keys always go to the same partition for a fixed number of partitions (e.g. if we had 5 partitions versus 3 that we set up here, we would get a different mapping. However if we set up another 5 partition

            // Send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // If the record is successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        // Display the exception
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // Block the send to make it synchronous - don't do this in production!
        }

        // Flush data - forces all data to be produced
        producer.flush();

        // Flush and close Producer
        producer.close();
    }
}