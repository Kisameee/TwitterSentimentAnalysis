package eu.nyuu.courses;

import eu.nyuu.courses.serdes.Consumer;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Main {

    public static void main(final String[] args) throws InterruptedException {
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
