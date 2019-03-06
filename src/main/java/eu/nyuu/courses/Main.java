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
}
