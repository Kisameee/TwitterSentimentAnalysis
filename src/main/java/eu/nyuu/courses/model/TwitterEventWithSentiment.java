package eu.nyuu.courses.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter

public class TwitterEventWithSentiment {

    public TwitterEventWithSentiment(TwitterEvent twitterEvent, String sentiment){
        this.id = twitterEvent.getId();
        this.timestamp = twitterEvent.getTimestamp();
        this.nick = twitterEvent.getNick();
        this.body = twitterEvent.getBody();
        this.sentiment = sentiment;

    }
    private String id;
    private String timestamp;
    private String nick;
    private String body;
    private String sentiment;

}
