package it.uniroma2.sabd.christiansantapaola;

import com.google.gson.Gson;
import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

public class Query2Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SABD[01].Query2")
                .set("spark.hadoop.validateOutputSpecs", "false");
        // JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> csvData = spark.read().textFile("/out500_combined+header.csv").toJavaRDD();
        JavaRDD<DataRow> rawData = csvData
                .filter(line -> line.length() > 1)
                .filter(line -> line.charAt(0) != '#')
                .map(line -> line.split(","))
                .map(fields -> {
                    return new DataRow(fields[0], fields[1], fields[2], fields[3], fields[21], fields[23], fields[26]);
                });

        Query2 query2 = new Query2(sc);
        JavaPairRDD<Tuple2<String, LocalDate>, Tuple2<Double, Long>> q2ValueByHour = query2.getDiffTimeSeries(rawData);
        JavaRDD<Q2Out> res2 = query2.extractStats(q2ValueByHour);
        res2.map(Q2Out::toCSV).coalesce(1).saveAsTextFile("/Results/q2.csv");

        List<Tuple2<Tuple2<String, LocalDate>, Double>> best5 = q2ValueByHour
                .mapToPair(pair -> new Tuple2<>(pair._1(), pair._2()._1()))
                .groupByKey()
                .mapValues(iterable -> {
                    return StreamSupport.stream(iterable.spliterator(), false).max(Double::compare).orElse(Double.NaN);
                })
                .mapToPair(pair -> new Tuple2<>(pair._1()._2(), new Tuple2<>(pair._1()._1(), pair._2())))
                .groupByKey()
                .flatMapValues(iterable -> {
                    ArrayList<Tuple2<String, Double>> arr = new ArrayList<>();
                    iterable.forEach(arr::add);
                    arr.sort(Comparator.comparing(Tuple2::_2));
                    ArrayList<Tuple2<String, Double>> res = new ArrayList<>(5);
                    for (int i = 0; i < 5; i++) {
                        try {
                            res.add(arr.get(arr.size() - 1 - i));
                        } catch (IndexOutOfBoundsException ignored) {

                        }
                    }
                    return res.iterator();
                })
                .mapToPair(pair -> new Tuple2<>(new Tuple2<>(pair._2()._1(), pair._1()), pair._2()._2()))
                .collect();

        List<Tuple2<Tuple2<String, LocalDate>, Double>> worst5 = q2ValueByHour
                .mapToPair(pair -> new Tuple2<>(pair._1(), pair._2()._1()))
                .groupByKey()
                .mapValues(iterable -> {
                    return StreamSupport.stream(iterable.spliterator(), false).min(Double::compare).orElse(Double.NaN);
                })
                .mapToPair(pair -> new Tuple2<>(pair._1()._2(), new Tuple2<>(pair._1()._1(), pair._2())))
                .groupByKey()
                .flatMapValues(iterable -> {
                    ArrayList<Tuple2<String, Double>> arr = new ArrayList<>();
                    iterable.forEach(arr::add);
                    arr.sort(Comparator.comparing(Tuple2::_2));
                    ArrayList<Tuple2<String, Double>> res = new ArrayList<>(5);
                    for (int i = 0; i < 5; i++) {
                        try {
                            res.add(arr.get(i));
                        } catch (IndexOutOfBoundsException ignored) {

                        }
                    }
                    return res.iterator();
                })
                .mapToPair(pair -> new Tuple2<>(new Tuple2<>(pair._2()._1(), pair._1()), pair._2()._2()))
                .collect();

        sc.parallelize(best5).coalesce(1).map(pair -> String.format("%s,%s,%f", pair._1()._1(), pair._1()._2(), pair._2())).saveAsTextFile("/Results/best5.csv");
        sc.parallelize(worst5).coalesce(1).map(pair -> String.format("%s,%s,%f", pair._1()._1(), pair._1()._2(), pair._2())).saveAsTextFile("/Results/worst5.csv");

        RedisConfig redisConfig = RedisConfig.fromSparkConf(spark.sparkContext().conf());
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(spark.sparkContext().conf());
        RedisContext redisContext = new RedisContext(spark.sparkContext());
        RDD<Tuple2<String, String>> q2ToRedis = res2.mapToPair(q2 -> new Tuple2<>("sabd.query2." + q2.getID(), q2)).groupByKey().mapValues(
                iterable -> {
                    ArrayList<Q2Out> vals = new ArrayList<>();
                    iterable.forEach(vals::add);
                    Gson gson = new Gson();
                    return gson.toJson(vals);
                }
        ).rdd();
        redisContext.toRedisKV(q2ToRedis, 0,  redisConfig, readWriteConfig);
        redisContext.toRedisKV(
                sc.parallelize(best5).map(pair -> new Tuple2<>("sabd.worst5." + pair._1(), Double.toString(pair._2()))).rdd(),
                0, redisConfig, readWriteConfig
        );

        redisContext.toRedisKV(
                sc.parallelize(worst5).map(pair -> new Tuple2<>("sabd.best5." + pair._1(), Double.toString(pair._2()))).rdd(),
                0, redisConfig, readWriteConfig
        );


        spark.close();
    }
}
