package eu.nyuu.courses;
//
//import eu.nyuu.courses.Consumer.Consumer;
//import lombok.extern.slf4j.Slf4j;
//
//
//@Slf4j
//public class Main {
//
//    public static void main(final String[] args) throws InterruptedException {
//        Consumer consumer = new Consumer();
//        consumer.start();
//        consumer.join();
//    }
//}


import eu.nyuu.courses.Consumer.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
        new Consumer();
    }
}