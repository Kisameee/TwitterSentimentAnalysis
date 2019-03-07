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
        this.positive = metricEvent.getPositive();
        this.negative = metricEvent.getNegative();
        this.neutral = metricEvent.getNeutral();
        if (sentiment.getSentiment().equals("POSITIVE") || sentiment.getSentiment().equals("VERY_POSITIVE")) {
            this.setPositive(metricEvent.getPositive() + 1);
        } else if (sentiment.getSentiment().equals("NEGATIVE") || sentiment.getSentiment().equals("VERY_NEGATIVE")) {
            this.setNegative(metricEvent.getNegative() + 1);
        } else {
            this.setNeutral(metricEvent.getNeutral() + 1);
        }
    }

    @Override
    public String toString() {
        return "Negative = " + Integer.toString(negative) + " Positive = " + Integer.toString(positive) + " Neutral = " + Integer.toString(neutral) ;
    }
    public void append(MetricEvent metricEvent){
        this.positive = this.positive + metricEvent.getPositive();
        this.negative = this.negative + metricEvent.getNegative();
        this.neutral = this.neutral + metricEvent.getNeutral();
    }
}

