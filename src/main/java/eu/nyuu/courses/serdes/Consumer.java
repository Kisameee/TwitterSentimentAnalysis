package eu.nyuu.courses.serdes;

import edu.stanford.nlp.simple.*;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.simple.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.WindowStore;


public class Consumer extends Thread{
    Thread t;
    
    public void run() {

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

        // Here you go :)

        final KStream<String, TwitterEvent> twitterStream = builder
                .stream("tweets", Consumed.with(stringSerde, TwitterEventSerde));


        KStream<String, TwitterEventWithSentiment> result = twitterStream
                .map((key, value) -> KeyValue.pair(value.getNick(), new TwitterEventWithSentiment(value, new Sentence(value.getBody()).sentiment().toString())));

        KTable<Windowed<String>, MetricEvent> user = result.groupByKey(Grouped.with(Serdes.String(), SerdeFactory.createSerde(TwitterEventWithSentiment.class, serdeProps)))
                .windowedBy(TimeWindows.of(Duration.ofMillis(10000)))
                .aggregate(
                        () -> new MetricEvent(),
                        (aggKey, newValue, aggValue) -> new MetricEvent(aggValue, newValue),

                        Materialized
                                .<String, MetricEvent, WindowStore< Bytes, byte[]>> as ("twitterStore").withValueSerde(MetricEventSerde)
                );

        user.toStream().print(Printed.toSysOut());


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
    }
    public Consumer(){
        run();
    }
}