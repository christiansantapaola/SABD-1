package it.uniroma2.sabd.christiansantapaola;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.collection.Seq;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Objects;

public class Query1 {
    JavaSparkContext sc;

    public Query1(JavaSparkContext sc) {
        this.sc = sc;
    }

    public JavaRDD<Q1Out> doQuery(JavaRDD<DataRow> data) {
        JavaRDD<DataRow> filteredData = data
                .filter(Objects::nonNull)
                .filter(dataRow -> !Double.isNaN(dataRow.getLast()))
                .distinct();

        JavaPairRDD<Tuple2<String, LocalDateTime>, Tuple2<Long, ArrayList<Double>>> q1Final = filteredData
                .filter(dataRow -> dataRow.getLastTrading() != null)
                .filter(dataRow -> Objects.equals(dataRow.getNationID(), "FR"))
                .filter(dataRow -> Objects.equals(dataRow.getSectype(), "E"))
                .groupBy(dataRow -> {
                    return new Tuple2<>(dataRow.getFullID(), dataRow.getLastTrading()
                            .withNano(0).withSecond(0).withMinute(0));
                })
                .mapValues(dataRows -> {
                    double res = 0.0;
                    long count = 0;
                    ArrayList<Double> dataRowsArray = new ArrayList<>();
                    dataRows.forEach(dataRow -> dataRowsArray.add(dataRow.getLast()));
                    double avg = dataRowsArray.stream().reduce(Double::sum).get();
                    double min = dataRowsArray.stream().min(Double::compareTo).get();
                    double max = dataRowsArray.stream().min(Double::compareTo).get();
                    ArrayList<Double> resultArr = new ArrayList<>();
                    resultArr.add(0, avg);
                    resultArr.add(1, max);
                    resultArr.add(2, min);
                    return new Tuple2<>(count, resultArr);
                });
        return q1Final.map(pair -> {
            return new Q1Out(pair._1._1(), pair._1()._2(), pair._2()._1(), pair._2()._2().get(0),pair._2()._2().get(1),pair._2()._2().get(2));
        });
    }
}
