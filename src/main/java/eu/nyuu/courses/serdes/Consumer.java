package eu.nyuu.courses.serdes;

import eu.nyuu.courses.model.TwitterEvent;
import eu.nyuu.courses.model.MetricEvent;
import eu.nyuu.courses.model.TwitterEventWithSentiment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.simple.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.*;
@SuppressWarnings("Duplicates")


public class Consumer extends Thread{
    Thread t;

    public void run() {

        final String bootstrapServers = "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "a-ke-kikou");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka_streams");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "my-stream0.0.0..-app-client");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Where to find Kafka broker(s)
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<TwitterEvent> TwitterEventSerde = SerdeFactory.createSerde(TwitterEvent.class, serdeProps);
        final Serde<MetricEvent> MetricEventSerde = SerdeFactory.createSerde(MetricEvent.class, serdeProps);

        // Stream
        final StreamsBuilder builder = new StreamsBuilder();

        // Here you go :)

        final KStream<String, TwitterEvent> twitterStream = builder
                .stream("tweets", Consumed.with(stringSerde, TwitterEventSerde));

        // aggregation par user
        KStream<String, TwitterEventWithSentiment> result_user = twitterStream
                .map((key, value) -> KeyValue.pair(value.getNick(), new TwitterEventWithSentiment(value, new Sentence(value.getBody()).sentiment().toString())));

        KTable<Windowed<String>, MetricEvent> user = result_user.groupByKey(Grouped.with(Serdes.String(), SerdeFactory.createSerde(TwitterEventWithSentiment.class, serdeProps)))
                .windowedBy(TimeWindows.of(Duration.ofMillis(10000)))
                .aggregate(
                        () -> new MetricEvent(),
                        (aggKey, newValue, aggValue) -> new MetricEvent(aggValue, newValue),

                        Materialized
                                .<String, MetricEvent, WindowStore< Bytes, byte[]>> as ("twitterStoreUser").withValueSerde(MetricEventSerde)
                );
        user.toStream().print(Printed.toSysOut());


        // aggregation par jour
        KStream<String, TwitterEventWithSentiment> result_timestamp = twitterStream
                .map((key, value) -> KeyValue.pair("KEY", new TwitterEventWithSentiment(value, new Sentence(value.getBody()).sentiment().toString())));

        KTable<Windowed<String>, MetricEvent> timestamp = result_timestamp.groupByKey(Grouped.with(Serdes.String(), SerdeFactory.createSerde(TwitterEventWithSentiment.class, serdeProps)))
                .windowedBy(TimeWindows.of(Duration.ofMillis(10000)))
                .aggregate(
                        () -> new MetricEvent(),
                        (aggKey, newValue, aggValue) -> new MetricEvent(aggValue, newValue),
                        Materialized
                                .<String, MetricEvent, WindowStore<Bytes, byte[]>>as("twitterStore_date").withValueSerde(MetricEventSerde)
                );
        timestamp.toStream().print(Printed.toSysOut());


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            Thread.sleep(Duration.ofMinutes(1).toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (true) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                // Querying our local store
                ReadOnlyWindowStore<String, MetricEvent> windowStore =
                        streams.store("twitterStore_date", QueryableStoreTypes.windowStore());

                Instant now = Instant.now();
                // fetching all values for the last day/month/year in the window
                Instant lastMinute = now.minus(Duration.ofDays(1));
                Instant lastMonth = now.minus(Duration.ofDays(30));
                Instant lastYear = now.minus(Duration.ofDays(365));

                WindowStoreIterator<MetricEvent> iterator = windowStore.fetch("KEY", lastMinute, now);
                MetricEvent dayMetricEvent = new MetricEvent();
                while (iterator.hasNext()) {
                    KeyValue<Long, MetricEvent> next = iterator.next();
                    dayMetricEvent.append(next.value);
                }
                // close the iterator to release resources
                iterator.close();
                System.out.println("DEBUG Last minute : " + dayMetricEvent.toString());

                // fetching all values for the last month in the window
                iterator = windowStore.fetch("KEY", lastMonth, now);
                MetricEvent monthMetricEvent = new MetricEvent();
                while (iterator.hasNext()) {
                    KeyValue<Long, MetricEvent> next = iterator.next();
                    monthMetricEvent.append(next.value);
                }
                // close the iterator to release resources
                iterator.close();
                System.out.println("DEBUG Last month : " + monthMetricEvent.toString());

                iterator = windowStore.fetch("KEY", lastYear, now);
                MetricEvent yearMetricEvent = new MetricEvent();
                while (iterator.hasNext()) {
                    KeyValue<Long, MetricEvent> next = iterator.next();
                    yearMetricEvent.append(next.value);
                }
                // close the iterator to release resources
                iterator.close();
                System.out.println("DEBUG Last year : " + yearMetricEvent.toString());

            }

            // Dumping all keys every minute
            try {
                Thread.sleep(Duration.ofMinutes(1).toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}