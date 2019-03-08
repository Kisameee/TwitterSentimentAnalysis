package eu.nyuu.courses.model;


import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import eu.nyuu.courses.model.TwitterEvent;
import eu.nyuu.courses.model.Greeting;
import eu.nyuu.courses.serdes.Consumer;
import eu.nyuu.courses.serdes.SerdeFactory;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;


/**
 *
 * @author laghoual
 */
@RestController
public class GreetingController {
     private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private Consumer cs;
    
    // mes changement pour interactive query
      final String bootstrapServers = "51.15.90.153:9092";
      final Properties streamsConfiguration = new Properties();

    /*
    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return new Greeting(counter.incrementAndGet(),
                            String.format(template, name));
    } 
*/
      /*
    @RequestMapping("/TwitterEvent")
    public Consumer TwitterEvent(@RequestParam(value="name", defaultValue="TwitterEvent") String name) {
        return new Consumer();
    }
*/
    @RequestMapping("/TwitterEvent")
    public void TwitterEvent(@RequestParam(value="name", defaultValue="TwitterEvent") String name) {
        /*
        
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
    */
        // Stream
        final StreamsBuilder builder = new StreamsBuilder();

        // Here you go :)

         builder.table("twitterStore");   // on specifie le nom du store
        
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();
        
        ReadOnlyWindowStore<String, Long> windowStore =
        streams.store("twitterStore", QueryableStoreTypes.windowStore());
        
         Instant timeFrom = Instant.ofEpochMilli(0); // beginning of time = oldest available
       Instant timeTo = Instant.now(); // now (in processing-time)
       
     KeyValueIterator<Windowed<java.lang.String>,Long> iterator = windowStore.all();
     while (iterator.hasNext()) {
     KeyValue<Windowed<java.lang.String>,Long> next = iterator.next();
            Windowed<java.lang.String> windowTimestamp = next.key;
    System.out.println("Count of 'url' @ time " + windowTimestamp + " key= "+next.key+"  value = " + next.value);
    }
// close the iterator to release resources
    iterator.close();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
    
}
