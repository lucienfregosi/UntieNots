package fr.lucien.test;


import fr.lucien.test.Themes;
import fr.lucien.test.Theme;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaConsumer {
    final static Logger logger = Logger.getLogger(KafkaConsumer.class.getName());

    public static void main(String[] args) {
        // Kafka Broker (docker)
        String bootstrapServers = "127.0.0.1:9092";
        // String bootstrapServers = "192.168.99.100:9092"; // if docker machine

        // Schema registry (docker)
        String schemaRegistryServer = "http://127.0.0.1:8081";
        // String schemaRegistryServer = "http://192.168.99.100:8081"; // if docker machine

        // Create properties for the consumer
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testin");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", schemaRegistryServer);
        properties.put("specific.avro.reader", "true");

        // Create consumer
        KafkaConsumer<String, Themes> consumerThemes = new KafkaConsumer<String, Themes>(properties);
        KafkaConsumer<String, Theme> consumerTheme = new KafkaConsumer<String, Theme>(properties);

        // Subscribe a topic
        consumer.subscribe(Arrays.asList("theme", "themes"));

        // poll data
        while(true) {
            ConsumerRecords<String, Themes> recordsThemes = consumerThemes.poll(Duration.ofMillis(100));
            ConsumerRecords<String, Theme> recordsTheme = consumerTheme.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                logger.info("Key : " + record.key() + "; value : " + record.value());
                logger.info("Partition : " + record.partition() + "; offset : " + record.offset());
            }

        }

    }

}
