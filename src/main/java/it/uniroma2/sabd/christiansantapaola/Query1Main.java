package it.uniroma2.sabd.christiansantapaola;

import com.google.gson.Gson;
import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;

public class Query1Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SABD[01].Query1")
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

        Query1 query1 = new Query1(sc);
        JavaRDD<Q1Out> res1 = query1.doQuery(rawData);
        res1.map(Q1Out::toCSV).coalesce(1).saveAsTextFile("/Results/q1.csv");
        RedisConfig redisConfig = RedisConfig.fromSparkConf(spark.sparkContext().conf());
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(spark.sparkContext().conf());
        RedisContext redisContext = new RedisContext(spark.sparkContext());
        RDD<Tuple2<String, String>> q1ToRedis = res1.mapToPair(q1 -> new Tuple2<>("sabd.query1." + q1.getID(), q1)).groupByKey().mapValues(
                iterable -> {
                    ArrayList<Q1Out> vals = new ArrayList<>();
                    iterable.forEach(vals::add);
                    Gson gson = new Gson();
                    return gson.toJson(vals);
                }
        ).rdd();
        redisContext.toRedisKV(q1ToRedis, 0,  redisConfig, readWriteConfig);
        spark.close();
    }
}
