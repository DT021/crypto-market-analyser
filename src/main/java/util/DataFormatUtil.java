package util;

import bean.CurrencyPairPrice;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataFormatUtil implements Serializable {

    public JavaPairRDD<Integer, CurrencyPairPrice> getDataAsObjectPair(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, String APP_NAME, String IP){
        ArrayList<Integer> keys = new ArrayList<>();

        Iterator<Integer> keyIterator = data.keys().toLocalIterator();
        while (keyIterator.hasNext()){
            keys.add(keyIterator.next());
        }

        List<Tuple2<Integer, ArrayList<CurrencyPairPrice>>> dataAsTuple = new ArrayList<>();
        Iterator<Iterable<CurrencyPairPrice>> currencyPairPriceIterator = data.values().toLocalIterator();

        int keycounter = 0;
        while (currencyPairPriceIterator.hasNext()){
            ArrayList<CurrencyPairPrice> values = new ArrayList<>();
            Iterator<CurrencyPairPrice> iterator = currencyPairPriceIterator.next().iterator();
            while (iterator.hasNext()){
                values.add(iterator.next());
            }

            dataAsTuple.add(new Tuple2<Integer, ArrayList<CurrencyPairPrice>>(keys.get(keycounter++), values));
        }

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(IP);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

        List<Tuple2<Integer, CurrencyPairPrice>> tupleToJavaPairRDD = new ArrayList<>();
        for(int i=0; i<keys.size(); i++){
            for (CurrencyPairPrice value : dataAsTuple.get(i)._2){
                tupleToJavaPairRDD.add(new Tuple2<Integer, CurrencyPairPrice>(keys.get(i), value));
            }
        }

        return sc.parallelizePairs(tupleToJavaPairRDD);
    }

    public JavaPairRDD<Integer, Double> getDataAsJavaPairRDD(JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> data, int type, String APP_NAME, String IP){

        ArrayList<Integer> keys = new ArrayList<>();

        Iterator<Integer> keyIterator = data.keys().toLocalIterator();
        while (keyIterator.hasNext()){
            keys.add(keyIterator.next());
        }

        List<Tuple2<Integer, ArrayList<Double>>> dataAsTuple = new ArrayList<>();
        Iterator<Iterable<CurrencyPairPrice>> currencyPairPriceIterator = data.values().toLocalIterator();

        int keycounter = 0;
        while (currencyPairPriceIterator.hasNext()){
            ArrayList<Double> values = new ArrayList<>();
            Iterator<CurrencyPairPrice> iterator = currencyPairPriceIterator.next().iterator();
            if(type == 1){
                while (iterator.hasNext()){
                    values.add(iterator.next().getValue());
                }
            }else if(type == 2){
                while (iterator.hasNext()){
                    values.add(iterator.next().getBaseVolume());
                }
            }else {
                while (iterator.hasNext()){
                    values.add(iterator.next().getQuoteVolume());
                }
            }

            dataAsTuple.add(new Tuple2<Integer, ArrayList<Double>>(keys.get(keycounter++), values));
        }

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(IP);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

        List<Tuple2<Integer, Double>> tupleToJavaPairRDD = new ArrayList<>();
        for(int i=0; i<keys.size(); i++){
            for (Double value : dataAsTuple.get(i)._2){
                tupleToJavaPairRDD.add(new Tuple2<Integer, Double>(keys.get(i), value));
            }
        }

        return sc.parallelizePairs(tupleToJavaPairRDD);
    }

    public JavaPairRDD<Integer, CurrencyPairPrice> getCurrentValues(JavaPairRDD<Integer, CurrencyPairPrice> values){
        JavaPairRDD<Integer, CurrencyPairPrice> reducedvaleus = values.reduceByKey(new Function2<CurrencyPairPrice, CurrencyPairPrice, CurrencyPairPrice>() {
            @Override
            public CurrencyPairPrice call(CurrencyPairPrice currencyPairPrice, CurrencyPairPrice currencyPairPrice2) throws Exception {
                if(currencyPairPrice.getTimeStamp().compareTo(currencyPairPrice2.getTimeStamp()) > 0){
                    return currencyPairPrice;
                }else {
                    return currencyPairPrice2;
                }
            }
        });

        return reducedvaleus;
    }

    public JavaPairRDD<Integer, CurrencyPairPrice> getOldestValues(JavaPairRDD<Integer, CurrencyPairPrice> values){
        JavaPairRDD<Integer, CurrencyPairPrice> reducedvaleus = values.reduceByKey(new Function2<CurrencyPairPrice, CurrencyPairPrice, CurrencyPairPrice>() {
            @Override
            public CurrencyPairPrice call(CurrencyPairPrice currencyPairPrice, CurrencyPairPrice currencyPairPrice2) throws Exception {
                if(currencyPairPrice.getTimeStamp().compareTo(currencyPairPrice2.getTimeStamp()) < 0){
                    return currencyPairPrice;
                }else {
                    return currencyPairPrice2;
                }
            }
        });

        return reducedvaleus;
    }

    public JavaPairRDD<Integer, Double> getIntegerDoubleJavaPairRDD(ArrayList<Integer> keys, ArrayList<Double> changes, String APP_NAME, String IP) {
        List<Tuple2<Integer, Double>> tupleToJavaPairRDD = new ArrayList<>();

        if(keys.size() > 0 && changes.size() > 0){
            for(int i=0; i<keys.size(); i++){
                new Tuple2<Integer, Double>(keys.get(i), changes.get(i));
            }
        }

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(IP);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

        return sc.parallelizePairs(tupleToJavaPairRDD);
    }

    public JavaPairRDD<Integer, Double> objectToDouble(JavaPairRDD<Integer, CurrencyPairPrice> data, String APP_NAME, String IP){
        Iterator<Tuple2<Integer, CurrencyPairPrice>> iterator = data.toLocalIterator();

        ArrayList<Integer> keys = new ArrayList<>();
        ArrayList<Double> values = new ArrayList<>();
        while (iterator.hasNext()){
            Tuple2<Integer, CurrencyPairPrice> tuple = iterator.next();
            keys.add(tuple._1);
            values.add(tuple._2.getValue());
        }

        return getIntegerDoubleJavaPairRDD(keys, values, APP_NAME, IP);
    }

    public ArrayList<Integer> extractKeys(JavaPairRDD<Integer, CurrencyPairPrice> data){
        ArrayList<Integer> keys = new ArrayList<>();

        Iterator<CurrencyPairPrice> keyIterator = data.values().toLocalIterator();
        while (keyIterator.hasNext()){
            keys.add(keyIterator.next().getId());
        }

        return keys;
    }

    public ArrayList<Double> extractValues(JavaPairRDD<Integer, CurrencyPairPrice> data){
        ArrayList<Double> values = new ArrayList<>();

        Iterator<CurrencyPairPrice> keyIterator = data.values().toLocalIterator();
        while (keyIterator.hasNext()){
            values.add(keyIterator.next().getValue());
        }

        return values;
    }

}
