package bean;

public class CurrencyPair {

    private Integer id;
    private double average;
    private double min;
    private double max;
    private double change;
    private double baseVolume;
    private double quoteVolume;
    private double standardDeviation;

    public CurrencyPair() {
    }

    public CurrencyPair(Integer id, double average, double min, double max, double change, double baseVolume, double quoteVolume, double standardDeviation) {
        this.id = id;
        this.average = average;
        this.min = min;
        this.max = max;
        this.change = change;
        this.baseVolume = baseVolume;
        this.quoteVolume = quoteVolume;
        this.standardDeviation = standardDeviation;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public double getBaseVolume() {
        return baseVolume;
    }

    public void setBaseVolume(double baseVolume) {
        this.baseVolume = baseVolume;
    }

    public double getQuoteVolume() {
        return quoteVolume;
    }

    public void setQuoteVolume(double quoteVolume) {
        this.quoteVolume = quoteVolume;
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
                "id=" + id +
                ", average=" + average +
                ", min=" + min +
                ", max=" + max +
                ", change=" + change +
                ", baseVolume=" + baseVolume +
                ", quoteVolume=" + quoteVolume +
                ", standardDeviation=" + standardDeviation +
                '}';
    }
}
