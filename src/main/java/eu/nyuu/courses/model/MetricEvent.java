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
        if (sentiment.getSentiment().equals("POSITIVE") || sentiment.getSentiment().equals("VERY_POSITIVE")) {
            this.setPositive(getPositive() + 1);
        } else if (sentiment.getSentiment().equals("NEGATIVE") || sentiment.getSentiment().equals("VERY_NEGATIVE")) {
            this.setNegative(getNegative() + 1);
        } else {
            this.setNeutral(getNeutral() + 1);
        }
    }

    @Override
    public String toString() {
        return "Negative = " + Integer.toString(negative) + " Positive = " + Integer.toString(positive) + " Neutral = " + Integer.toString(neutral) ;
    }
}

