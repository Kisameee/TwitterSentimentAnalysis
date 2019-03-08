package eu.nyuu.courses.Controller;


import edu.stanford.nlp.simple.Sentence;
import eu.nyuu.courses.model.MetricEvent;
import eu.nyuu.courses.model.TwitterEvent;
import eu.nyuu.courses.model.TwitterEventWithSentiment;
import eu.nyuu.courses.serdes.SerdeFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.BadRequestException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Getter
@Setter
@ToString
@RestController
@SuppressWarnings("Duplicates")
public class GlobalController {
    private KafkaStreams streams;


    public GlobalController() {

        final String bootstrapServers = "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "a-ke-kikou");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\tmp\\kafka-streams\\TWEET_LAGHOUAL_AY\\0_0");
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
                                .<String, MetricEvent, WindowStore<Bytes, byte[]>>as("twitterStoreUser").withValueSerde(MetricEventSerde)
                );
//        user.toStream().print(Printed.toSysOut());


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
//        timestamp.toStream().print(Printed.toSysOut());


        this.streams = new KafkaStreams(builder.build(), streamsConfiguration);
        this.streams.cleanUp();
        this.streams.start();

    }


    @GetMapping("/store/lastday")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MetricEvent get() {
        if (streams.state() == KafkaStreams.State.RUNNING) {
            // Querying our local store
            ReadOnlyWindowStore<String, MetricEvent> windowStore =
                    streams.store("twitterStore_date", QueryableStoreTypes.windowStore());
            Instant now = Instant.now();
            // fetching all values for the last day/month/year in the window
            Instant lastDay = now.minus(Duration.ofDays(1));

            WindowStoreIterator<MetricEvent> iterator = windowStore.fetch("KEY", lastDay, now);
            MetricEvent dayMetricEvent = new MetricEvent();
            while (iterator.hasNext()) {
                KeyValue<Long, MetricEvent> next = iterator.next();
                dayMetricEvent.append(next.value);
            }
            // close the iterator to release resources
            iterator.close();
            return dayMetricEvent;

        } else {
            throw new BadRequestException();
        }
    }

    @GetMapping("/store/")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void getDefault() {
        if (streams.state() == KafkaStreams.State.RUNNING) {

            log.debug("Your stream starded correctly");

        } else {
            throw new BadRequestException();
        }
    }


    @GetMapping("/store/lastmonth")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MetricEvent get2() {
        if (streams.state() == KafkaStreams.State.RUNNING) {
            // Querying our local store
            ReadOnlyWindowStore<String, MetricEvent> windowStore =
                    streams.store("twitterStore_date", QueryableStoreTypes.windowStore());
            Instant now = Instant.now();
            // fetching all values for the last day/month/year in the window
            Instant lastMonth = now.minus(Duration.ofDays(30));
            WindowStoreIterator<MetricEvent> iterator = windowStore.fetch("KEY", lastMonth, now);
            MetricEvent monthMetricEvent = new MetricEvent();
            while (iterator.hasNext()) {
                KeyValue<Long, MetricEvent> next = iterator.next();
                monthMetricEvent.append(next.value);
            }
            // close the iterator to release resources
            iterator.close();
            return monthMetricEvent;

        } else {
            throw new BadRequestException();
        }
    }


    @GetMapping("/store/lastyear")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MetricEvent get3() {
        if (streams.state() == KafkaStreams.State.RUNNING) {
            // Querying our local store
            ReadOnlyWindowStore<String, MetricEvent> windowStore =
                    streams.store("twitterStore_date", QueryableStoreTypes.windowStore());
            Instant now = Instant.now();
            // fetching all values for the last day/month/year in the window
            Instant lastYear = now.minus(Duration.ofDays(365));

            WindowStoreIterator<MetricEvent> iterator = windowStore.fetch("KEY", lastYear, now);
            MetricEvent yearMetricEvent = new MetricEvent();
            while (iterator.hasNext()) {
                KeyValue<Long, MetricEvent> next = iterator.next();
                yearMetricEvent.append(next.value);
            }
            // close the iterator to release resources
            iterator.close();
            return yearMetricEvent;

        } else {
            throw new BadRequestException();
        }
    }


    @GetMapping("/store/user")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MetricEvent get4() {
        if (streams.state() == KafkaStreams.State.RUNNING) {
            // Querying our local store
            ReadOnlyWindowStore<String, MetricEvent> windowStore =
                    streams.store("twitterStoreUser", QueryableStoreTypes.windowStore());
            Instant now = Instant.now();
            // fetching all values for the last day/month/year in the window
            Instant lastDay = now.minus(Duration.ofDays(1));

            WindowStoreIterator<MetricEvent> iterator = windowStore.fetch("USERNAME", lastDay, now);
            MetricEvent day_userMetricEvent = new MetricEvent();
            while (iterator.hasNext()) {
                KeyValue<Long, MetricEvent> next = iterator.next();
                day_userMetricEvent.append(next.value);
            }
            // close the iterator to release resources
            iterator.close();
            return day_userMetricEvent;

        } else {
            throw new BadRequestException();
        }
    }
}