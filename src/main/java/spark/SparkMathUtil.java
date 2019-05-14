package spark;

import bean.CurrencyPairPrice;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

public class SparkMathUtil extends BasicSpark{

    public JavaPairRDD<Integer, Double> getMin(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, boolean isDaily){

        JavaPairRDD<Integer, CurrencyPairPrice> filteredData = filterData(this.dataFormatUtil.getDataAsObjectPair(data, APP_NAME, LOCAL_IP),
                (isDaily ? Calendar.DAY_OF_MONTH : Calendar.MONTH), -1);
        JavaPairRDD<Integer, Double> values = this.dataFormatUtil.objectToDouble(filteredData, APP_NAME, LOCAL_IP);
        JavaPairRDD<Integer, Double> minValues = values.reduceByKey(Math::min);

        return minValues;
    }

    public JavaPairRDD<Integer, Double> getMax(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, boolean isDaily){

        JavaPairRDD<Integer, CurrencyPairPrice> filteredData = filterData(this.dataFormatUtil.getDataAsObjectPair(data, APP_NAME, LOCAL_IP),
                (isDaily ? Calendar.DAY_OF_MONTH : Calendar.MONTH), -1);
        JavaPairRDD<Integer, Double> values = this.dataFormatUtil.objectToDouble(filteredData, APP_NAME, LOCAL_IP);
        JavaPairRDD<Integer, Double> maxValues = values.reduceByKey(Math::max);

        return maxValues;
    }

    public JavaPairRDD<Integer, Double> getAverage(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, boolean isDaily){

        JavaPairRDD<Integer, CurrencyPairPrice> filteredData = filterData(this.dataFormatUtil.getDataAsObjectPair(data, APP_NAME, LOCAL_IP),
                (isDaily ? Calendar.DAY_OF_MONTH : Calendar.MONTH), -1);

        JavaPairRDD<Integer, Double> doubleJavaPairRDD = this.dataFormatUtil.objectToDouble(filteredData, APP_NAME, LOCAL_IP);

        Function<Double, AvgCount> createAcc = new Function<Double, AvgCount>() {
            public AvgCount call(Double x) {
                return new AvgCount(x, 1);
            }
        };

        Function2<AvgCount, Double, AvgCount> addAndCount =
                new Function2<AvgCount, Double, AvgCount>() {
                    public AvgCount call(AvgCount a, Double x) {
                        a.total_ += x;
                        a.num_ += 1;
                        return a;
                    }
                };

        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) {
                        a.total_ += b.total_;
                        a.num_ += b.num_;
                        return a;
                    }
                };

        JavaPairRDD<Integer, AvgCount> avgCounts =
                doubleJavaPairRDD.combineByKey(createAcc, addAndCount, combine);

        long count = doubleJavaPairRDD.count();
        JavaPairRDD<Integer, Double> sumValues = doubleJavaPairRDD.reduceByKey((x, y) -> (x+y)/count);

        return sumValues;
    }

    public JavaPairRDD<Integer, Double> getVolume(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, boolean isBaseVolume, boolean isDaily){
        JavaPairRDD<Integer, CurrencyPairPrice> values = this.dataFormatUtil.getDataAsObjectPair(data, APP_NAME, LOCAL_IP);
        JavaPairRDD<Integer, CurrencyPairPrice> filteredValues = null;
        if(isDaily){
            filteredValues = filterData(values, Calendar.DAY_OF_MONTH, -1);
        }else {
            filteredValues = filterData(values, Calendar.MONTH, -1);
        }

        JavaPairRDD<Integer, CurrencyPairPrice> reducedvaleus = dataFormatUtil.getCurrentValues(filteredValues);

        ArrayList<Integer> keys = new ArrayList<>();
        ArrayList<Double> volumes = new ArrayList<>();
        Iterator<CurrencyPairPrice> valuesIterator = reducedvaleus.values().toLocalIterator();
        while (valuesIterator.hasNext()){
            if(isBaseVolume){
                volumes.add(valuesIterator.next().getBaseVolume());
            }else {
                volumes.add(valuesIterator.next().getQuoteVolume());
            }
        }

        Iterator<Integer> keysIterator = reducedvaleus.keys().toLocalIterator();
        while (keysIterator.hasNext()){
            keys.add(keysIterator.next());
        }

        return dataFormatUtil.getIntegerDoubleJavaPairRDD(keys, volumes, APP_NAME, LOCAL_IP);
    }

    public JavaPairRDD<Integer, Double> getChange(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, boolean isDaily){
        JavaPairRDD<Integer, CurrencyPairPrice> values = this.dataFormatUtil.getDataAsObjectPair(data, APP_NAME, LOCAL_IP);

        JavaPairRDD<Integer, CurrencyPairPrice> currentPrices = dataFormatUtil.getCurrentValues(values);
        JavaPairRDD<Integer, CurrencyPairPrice> aDayBeforePrices = dataFormatUtil.getOldestValues(filterData(values, (isDaily ? Calendar.DAY_OF_MONTH : Calendar.MONTH), -1));

        Iterator<CurrencyPairPrice> currentPriceIterator = currentPrices.values().toLocalIterator();
        Iterator<CurrencyPairPrice> oldPriceIterator = aDayBeforePrices.values().toLocalIterator();

        ArrayList<Integer> keys = dataFormatUtil.extractKeys(currentPrices);
        ArrayList<Double> changes = new ArrayList<>();
        while (currentPriceIterator.hasNext() && oldPriceIterator.hasNext()){
            double currentPrice = currentPriceIterator.next().getValue();
            double oldPrice = oldPriceIterator.next().getValue();
            changes.add(oldPrice != 0 ? currentPrice / oldPrice : 0);
        }

        return dataFormatUtil.getIntegerDoubleJavaPairRDD(keys, changes, APP_NAME, LOCAL_IP);
    }

    public JavaPairRDD<Integer, Double> getStandardDeviations(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> originalData, boolean isDaily){

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(LOCAL_IP);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

        JavaPairRDD<Integer, CurrencyPairPrice> objectVersionOfData = dataFormatUtil.getDataAsObjectPair(originalData, APP_NAME, LOCAL_IP);
        JavaPairRDD<Integer, CurrencyPairPrice> filteredData = null;
        if(isDaily){
            filteredData = filterData(objectVersionOfData, Calendar.DAY_OF_MONTH, -1);
        }else {
            filteredData = filterData(objectVersionOfData, Calendar.MONTH, -1);
        }

        List<Tuple2<Integer, Double>> tupleToJavaPairRDD = new ArrayList<>();
        Iterator<Tuple2<Integer, CurrencyPairPrice>> iterator = filteredData.toLocalIterator();
        while (iterator.hasNext()){
            Tuple2<Integer, CurrencyPairPrice> value =  iterator.next();
            tupleToJavaPairRDD.add(new Tuple2<>(value._1, value._2.getValue()));
        }

        JavaPairRDD<Integer, Double> wholeData = sc.parallelizePairs(tupleToJavaPairRDD);
        JavaPairRDD<Integer, StatCounter> standardDeviations = wholeData.aggregateByKey(new StatCounter(), StatCounter::merge, StatCounter::merge);

        ArrayList<Integer> keys = new ArrayList<>();
        ArrayList<Double> sdValues = new ArrayList<>();
        Iterator<Tuple2<Integer, StatCounter>> sdIterator = standardDeviations.toLocalIterator();
        while (sdIterator.hasNext()){
            Tuple2<Integer, StatCounter> tuple = sdIterator.next();
            keys.add(tuple._1);
            sdValues.add(tuple._2.stdev());
        }

        // We can get averages with this method as well
        /*while (sdIterator.hasNext()){
            Tuple2<Integer, StatCounter> tuple = sdIterator.next();
            keys.add(tuple._1);
            sdValues.add(tuple._2.mean());
        }*/

        return dataFormatUtil.getIntegerDoubleJavaPairRDD(keys, sdValues, APP_NAME, LOCAL_IP);
    }
    public static class AvgCount implements Serializable {
        public AvgCount(double total, int num) {
            total_ = total;
            num_ = num; }
        public double total_;
        public int num_;
        public double avg() {
            return total_ / (double) num_; }
    }

}
