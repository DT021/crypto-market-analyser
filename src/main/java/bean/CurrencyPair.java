package bean;

public class CurrencyPair {

    private double average;
    private double min;
    private double max;
    private double change;
    private double volume;
    private double standardDeviation;

    public CurrencyPair() {
    }

    public CurrencyPair(double average, double min, double max, double change, double volume, double standardDeviation) {
        this.average = average;
        this.min = min;
        this.max = max;
        this.change = change;
        this.volume = volume;
        this.standardDeviation = standardDeviation;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getChange() {
        return change;
    }

    public void setChange(double change) {
        this.change = change;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    @Override
    public String toString() {
        return "CurrencyPair{" +
                "average=" + average +
                ", min=" + min +
                ", max=" + max +
                ", change=" + change +
                ", volume=" + volume +
                ", standardDeviation=" + standardDeviation +
                '}';
    }
}
