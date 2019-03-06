package eu.nyuu.courses.model;

import lombok.Getter;
import lombok.Setter;


@Getter
@Setter

public class TwitterEvent {

    private String id;
    private String timestamp;
    private String nick;
    private String body;

}

