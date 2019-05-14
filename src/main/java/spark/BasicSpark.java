package spark;

import bean.CurrencyPair;
import bean.CurrencyPairPrice;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import util.DataFormatUtil;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;

public class BasicSpark implements Serializable {

    protected Integer PARTITION_COUNT = 2;
    protected String APP_NAME = "Crypto Analyser";
    protected String LOCAL_IP = "spark://192.168.1.124:7077";
    protected String CUSTOM_IP = "spark://192.168.1.124:7077";
    protected DataFormatUtil dataFormatUtil;

    public BasicSpark(){
        this.dataFormatUtil = new DataFormatUtil();
    }

    public ArrayList<CurrencyPair> getWholeData(boolean isDaily, String path){

        ArrayList<CurrencyPair> data = new ArrayList<>();
        SparkMathUtil mathUtil = new SparkMathUtil();
        int dateUnit = isDaily ? Calendar.DAY_OF_MONTH : Calendar.MONTH;
        JavaRDD<CurrencyPairPrice> rdd = readDataFile(path);

        JavaPairRDD<Integer, CurrencyPairPrice> rddPaired = rdd.mapToPair(new PairFunction<CurrencyPairPrice, Integer, CurrencyPairPrice>() {
            public Tuple2<Integer, CurrencyPairPrice> call(CurrencyPairPrice currencyPairPrice) throws Exception {
                Tuple2<Integer, CurrencyPairPrice>  tuple = new Tuple2<Integer, CurrencyPairPrice>(currencyPairPrice.getId(), currencyPairPrice);
                return tuple;
            }
        });

        rddPaired = filterData(rddPaired, dateUnit, -3);

        JavaPairRDD<Integer, Iterable<CurrencyPairPrice>> groupedData = rddPaired.groupByKey();

        if(isDaily){
            JavaPairRDD<Integer, Double> dailyMinValues = mathUtil.getMin(groupedData, true);
            JavaPairRDD<Integer, Double> dailyMaxValues = mathUtil.getMax(groupedData, true);
            JavaPairRDD<Integer, Double> dailyChange = mathUtil.getChange(groupedData, true);
            JavaPairRDD<Integer, Double> dailySd = mathUtil.getStandardDeviations(groupedData, true);
            JavaPairRDD<Integer, Double> baseVolume = mathUtil.getVolume(groupedData, true, true);
            JavaPairRDD<Integer, Double> quoteVolume = mathUtil.getVolume(groupedData, false, true);
            JavaPairRDD<Integer, Double> dailyAverageValues = mathUtil.getAverage(groupedData, true);

            extractDataAsList(data, dailyMinValues, dailyMaxValues, dailyChange, dailySd, baseVolume, quoteVolume, dailyAverageValues);
        }else {
            JavaPairRDD<Integer, Double> monthlyMinValues = mathUtil.getMin(groupedData, false);
            JavaPairRDD<Integer, Double> monthlyMaxValues = mathUtil.getMax(groupedData, false);
            JavaPairRDD<Integer, Double> monthlyAverageValues = mathUtil.getAverage(groupedData, false);
            JavaPairRDD<Integer, Double> baseVolume = mathUtil.getVolume(groupedData, true, false);
            JavaPairRDD<Integer, Double> quoteVolume = mathUtil.getVolume(groupedData, false, false);
            JavaPairRDD<Integer, Double> monthlyChange = mathUtil.getChange(groupedData, false);
            JavaPairRDD<Integer, Double> monthlySd = mathUtil.getStandardDeviations(groupedData, false);

            extractDataAsList(data, monthlyMinValues, monthlyMaxValues, monthlyChange, monthlySd, baseVolume, quoteVolume, monthlyAverageValues);
        }


        return data;
    }

    private void extractDataAsList(ArrayList<CurrencyPair> data, JavaPairRDD<Integer, Double> dailyMinValues, JavaPairRDD<Integer, Double> dailyMaxValues, JavaPairRDD<Integer, Double> dailyChange, JavaPairRDD<Integer, Double> dailySd, JavaPairRDD<Integer, Double> baseVolume, JavaPairRDD<Integer, Double> quoteVolume, JavaPairRDD<Integer, Double> dailyAverageValues) {
        HashMap<Integer, Double> min = dataFormatUtil.extractAsMap(dailyMinValues);
        HashMap<Integer, Double> max = dataFormatUtil.extractAsMap(dailyMaxValues);
        HashMap<Integer, Double> avg = dataFormatUtil.extractAsMap(dailyAverageValues);
        HashMap<Integer, Double> bVolume = dataFormatUtil.extractAsMap(baseVolume);
        HashMap<Integer, Double> qVolume = dataFormatUtil.extractAsMap(quoteVolume);
        HashMap<Integer, Double> change = dataFormatUtil.extractAsMap(dailyChange);
        HashMap<Integer, Double> sd = dataFormatUtil.extractAsMap(dailySd);

        Iterator<Integer> keyIterator = min.keySet().iterator();
        while (keyIterator.hasNext()){
            int key = keyIterator.next();

            CurrencyPair currencyPair = new CurrencyPair();
            currencyPair.setId(key);
            currencyPair.setMin(min.get(key));
            currencyPair.setMax(max.get(key));
            currencyPair.setAverage(avg.get(key));
            currencyPair.setBaseVolume(bVolume.get(key));
            currencyPair.setQuoteVolume(qVolume.get(key));
            currencyPair.setChange(change.get(key));
            currencyPair.setStandardDeviation(sd.get(key));

            data.add(currencyPair);
        }
    }

    public JavaRDD<CurrencyPairPrice> readDataFile(String path){
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(LOCAL_IP);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

        JavaRDD<CurrencyPairPrice> data = sc.textFile(path, PARTITION_COUNT).map(
                new Function<String, CurrencyPairPrice>() {
                    public CurrencyPairPrice call(String line) throws Exception {
                        String[] fields = line.split(","); // Split line from commas

                        // read each data into custom object
                        CurrencyPairPrice cp = new CurrencyPairPrice();
                        cp.setId(Integer.parseInt(fields[0].trim()));
                        cp.setValue(Double.parseDouble(fields[1].trim()));
                        cp.setBaseVolume(Double.parseDouble(fields[2].trim()));
                        cp.setQuoteVolume(Double.parseDouble(fields[3].trim()));
                        cp.setTimeStamp((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(fields[4].replaceAll("\"", "")));

                        return cp;
                    }
                }
        );

        return data;
    }

    public JavaPairRDD<Integer, CurrencyPairPrice> filterData(JavaPairRDD<Integer, CurrencyPairPrice> rdd, int timeUnit, int amount){
        Calendar calendar = Calendar.getInstance();
        calendar.add(timeUnit, amount);

        Function<Tuple2<Integer, CurrencyPairPrice>, Boolean> filter = new Function<Tuple2<Integer, CurrencyPairPrice>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, CurrencyPairPrice> integerCurrencyPairPriceTuple2) throws Exception {
                if(integerCurrencyPairPriceTuple2._2.getTimeStamp().compareTo(calendar.getTime()) > 0){
                    return true;
                }else {
                    return false;
                }
            }
        };

        JavaPairRDD<Integer, CurrencyPairPrice> filteredData = rdd.filter(filter);
        return filteredData;
    }

}
