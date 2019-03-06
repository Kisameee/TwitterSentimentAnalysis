package eu.nyuu.courses;

import eu.nyuu.courses.model.TwitterEvent;
import eu.nyuu.courses.serdes.Consumer;
import eu.nyuu.courses.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class Main {

    public static void main(final String[] args) throws InterruptedException {
//        final String bootstrapServers = args.length > 0 ? args[0] : "163.172.145.138:9092";
//        final Properties streamsConfiguration = new Properties();
//
//
//        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "a-ke-kikou");
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Beerus\\.IntelliJIdea2018.3\\system\\tmp\\kafka-stream\\");
//        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "my-stream0.0.0..-app-client");
//        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//
//        // Where to find Kafka broker(s)
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        // Specify default (de)serializers for record keys and for record values
//        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


//        final Serde<String> stringSerde = Serdes.String();
//        final Map<String, Object> serdeProps = new HashMap<>();
//        final Serde<TwitterEvent> TwitterEventSerde = SerdeFactory.createSerde(TwitterEvent.class, serdeProps);
//
//        // Stream
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        // Here you go :)
//
//          final KStream<String, TwitterEvent> twitterStreams = builder
//                .stream("tweets", Consumed.with(stringSerde, TwitterEventSerde));

//        KTable<Windowed<String>, Long> result = visitsStream
//                .map((key, value) -> KeyValue.pair(value.getNick(), value))
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofMillis(10000)))
//                .count(Materialized.as("testStoreTweets"));

//        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
//
//        streams.cleanUp();
//        streams.start();
//        {
        Consumer consumer = new Consumer();
        consumer.start();
        consumer.join();

    }

//        // Get the window store named "CountsWindowStore"
//        ReadOnlyWindowStore<String, Long> windowStore =
//                streams.store("CountsWindowStore", QueryableStoreTypes.windowStore());
//
//// Fetch values for the key "world" for all of the windows available in this application instance.
//// To get *all* available windows we fetch windows from the beginning of time until now.
//        Instant timeFrom = Instant.ofEpochMilli(0); // beginning of time = oldest available
//        Instant timeTo = Instant.now(); // now (in processing-time)
//        WindowStoreIterator<Long> iterator = windowStore.fetch("world", timeFrom, timeTo);
//        while (iterator.hasNext()) {
//            KeyValue<Long, Long> next = iterator.next();
//            long windowTimestamp = next.key;
//            System.out.println("Count of 'world' @ time " + windowTimestamp + " is " + next.value);
//        }
//// close the iterator to release resources
//        iterator.close();
//
//        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

}
