package eu.nyuu.courses.model;


import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import eu.nyuu.courses.model.TwitterEvent;
import eu.nyuu.courses.model.Greeting;
import eu.nyuu.courses.serdes.Consumer;


/**
 *
 * @author laghoual
 */
@RestController
public class GreetingController {
     private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private Consumer cs;

    /*
    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return new Greeting(counter.incrementAndGet(),
                            String.format(template, name));
    } 
*/
    @RequestMapping("/TwitterEvent")
    public Consumer TwitterEvent(@RequestParam(value="name", defaultValue="Exemple") String name) {
        return new Consumer();
    }  
}
