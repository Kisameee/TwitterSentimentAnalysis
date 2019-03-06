package eu.nyuu.courses.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter

public class MetricEvent {

    private int negative;
    private int positive;
    private int neutral;

    public MetricEvent(){
        negative=0;
        positive=0;
        neutral=0;
    }
    public MetricEvent(MetricEvent metricEvent){
        negative=metricEvent.getNegative();
        positive=metricEvent.getPositive();
        neutral=metricEvent.getNeutral();
    }
    public MetricEvent(MetricEvent metricEvent, TwitterEventWithSentiment sentiment) {
        if (sentiment.getSentiment() == "positive") {
            this.setPositive(getPositive() + 1);
        } else if (sentiment.getSentiment() == "negative") {
            this.setNegative(getNegative() + 1);
        } else {
            this.setNeutral(getNeutral() + 1);
        }
    }
}

