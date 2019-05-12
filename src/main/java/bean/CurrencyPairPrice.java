package bean;

import java.io.Serializable;
import java.util.Date;

public class CurrencyPairPrice implements Serializable {

    private static final long serialVersionUID = -2685444218382696366L;
    private int id;
    private double value;
    private double baseVolume;
    private double quoteVolume;
    private Date timeStamp;

    public CurrencyPairPrice() { }

    public CurrencyPairPrice(int id, double value, double baseVolume, double quoteVolume, Date timeStamp) {
        this.id = id;
        this.value = value;
        this.baseVolume = baseVolume;
        this.quoteVolume = quoteVolume;
        this.timeStamp = timeStamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
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

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "CurrencyPair{" +
                "id=" + id +
                ", value=" + value +
                ", baseVolume=" + baseVolume +
                ", quoteVolume=" + quoteVolume +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
