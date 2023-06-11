package it.uniroma2.sabd.christiansantapaola;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class Query2 {
    JavaSparkContext sc;

    public Query2(JavaSparkContext sc) {
        this.sc = sc;
    }

    public JavaPairRDD<String, Tuple2<LocalDateTime, Double>> getBaseData(JavaRDD<DataRow> data) {
        JavaRDD<DataRow> filteredData = data
                .filter(Objects::nonNull)
                .filter(dataRow -> !Double.isNaN(dataRow.getLast()))
                .distinct();

        return filteredData
                .filter(dataRow -> dataRow.getLastTrading() != null)
                .mapToPair(dataRow -> {
                    return new Tuple2<>(
                            new Tuple2<>(dataRow.getFullID(), dataRow.getLastTrading()),
                            dataRow.getLast()
                    );
                })
                .distinct()
                .mapToPair(pair -> {
                            return new Tuple2<>(
                                    pair._1()._1(),
                                    new Tuple2<>(pair._1()._2(), pair._2()));
                        }
                );
    }

    public JavaPairRDD<Tuple2<String, LocalDateTime>, Double> getDiffByHour(JavaPairRDD<String, Tuple2<LocalDateTime, Double>> base) {
        return base.mapToPair(pair -> {
                            return new Tuple2<>(
                                    new Tuple2<>(pair._1(),
                                            pair._2()._1()
                                                    .withNano(0)
                                                    .withSecond(0)
                                                    .withMinute(0)),
                                    new Tuple2<>(pair._2()._1().toLocalTime(), pair._2()._2()));
                        })
                        .groupByKey()
                        .flatMapValues(iterable -> {
                            ArrayList<Tuple2<LocalTime, Double>> values = new ArrayList<>();
                            iterable.forEach(values::add);
                            values.sort(Comparator.comparing(Tuple2::_1));
                            ArrayList<Double> diff = new ArrayList<>();
                            for (int i = 1; i < values.size(); i++) {
                                double d1 = values.get(i)._2();
                                double d2 = values.get(i - 1)._2();
                                double diffVal = (d2 - d1) * 1000;
                                diff.add(diffVal);
                            }
                            return diff.iterator();
                        });
    }

    public JavaRDD<Q2Out> extractStats(JavaPairRDD<Tuple2<String, LocalDate>, Tuple2<Double, Long>> data) {
        JavaPairRDD<Tuple2<String, LocalDate>, Tuple2<Long, Tuple2<Double, Double>>> q2Final = data
                .groupByKey()
                .mapValues(iterable -> {
                    ArrayList<Tuple2<Double, Long>> input = new ArrayList<>();
                    iterable.forEach(input::add);
                    ArrayList<Double> diff = new ArrayList<>();
                    input.forEach(pair -> diff.add(pair._1()));
                double sum = diff.stream().reduce(Double::sum).orElse(0.0);
                long count = input.stream().reduce((t1, t2) -> new Tuple2<>(0.0, t1._2() + t2._2())).orElse(new Tuple2<>(0.0, 0L))._2();
                    double mean = sum / count;
                    double stdVar = diff.stream().reduce((d1, d2) -> d1 + (d2 - mean) * (d2 - mean)).orElse(0.0);
                    return new Tuple2<>(count, new Tuple2<>(mean, stdVar));
                });

        return q2Final.map(pair -> {
            return new Q2Out(pair._1()._1(), pair._1()._2(), pair._2()._1(), pair._2()._2()._1(), pair._2()._2()._1());
        });
    }

    public JavaPairRDD<Tuple2<String, LocalDate>, Tuple2<Double, Long>> getDiffTimeSeries(JavaRDD<DataRow> data) {
        JavaPairRDD<String, Tuple2<LocalDateTime, Double>> base = getBaseData(data);
        JavaPairRDD<Tuple2<String, LocalDateTime>, Tuple2<Double, Long>> valueByHour =
                base.mapToPair(pair -> {
                            return new Tuple2<>(
                                    new Tuple2<>(pair._1(),
                                            pair._2()._1()
                                                    .withNano(0)
                                                    .withSecond(0)
                                                    .withMinute(0)),
                                    new Tuple2<>(pair._2()._1().toLocalTime(), pair._2()._2()));
                        })
                        .groupByKey()
                        .mapToPair(pair -> {
                            ArrayList<Tuple2<LocalTime, Double>> values = new ArrayList<>();
                            pair._2().forEach(values::add);
                            Tuple2<LocalTime, Double> lastValueBeforeNextHour =
                                    values.stream().max(Comparator.comparing(Tuple2::_1)).orElse(new Tuple2<>(pair._1()._2().toLocalTime(), Double.NaN));
                            return new Tuple2<>(
                                    new Tuple2<>(pair._1()._1(), pair._1()._2().plusHours(1)),
                                    new Tuple2<>(lastValueBeforeNextHour._2(), (long) values.size())
                            );
                        });

        JavaPairRDD<Tuple2<String, LocalDateTime>, Tuple2<Double, Long>> missingDates =
                valueByHour
                        .mapToPair(
                                pair -> new Tuple2<>(pair._1()._1(), new Tuple2<>(pair._1()._2(), pair._2()._1()))
                        )
                        .groupByKey()
                        .mapValues(pair -> {
                            List<LocalDateTime> allDates = new ArrayList<>();
                            for (Tuple2<LocalDateTime, Double> tuple : pair) {
                                allDates.add(tuple._1());
                            }
                            List<LocalDateTime> missing = new ArrayList<>();
                            if (allDates.size() < 1) {
                                return missing;
                            }
                            LocalDateTime startDate = allDates.stream().min(LocalDateTime::compareTo).get();
                            LocalDateTime endDate = allDates.stream().max(LocalDateTime::compareTo).get();
                            for (LocalDateTime t = startDate; t.isBefore(endDate) || t.isEqual(endDate); t = t.plusHours(1)) {
                                if (!allDates.contains(t)) {
                                    missing.add(t);
                                }
                            }
                            return missing;
                        })
                        .flatMapToPair(pair -> {
                            ArrayList<Tuple2<Tuple2<String, LocalDateTime>, Tuple2<Double, Long>>> res = new ArrayList<>();
                            for (LocalDateTime date : pair._2()) {
                                res.add(new Tuple2<>(new Tuple2<>(pair._1(), date),
                                        new Tuple2<>(Double.NaN, 0L)));
                            }
                            return res.iterator();
                        });

        valueByHour = valueByHour.union(missingDates);
        JavaPairRDD<Tuple2<String, LocalDate>, Tuple2<Double, Long>> filledValue = valueByHour
                .mapToPair(pair -> {
                    Tuple2<String, LocalDate> key = new Tuple2<>(
                            pair._1()._1(),
                            pair._1()._2().toLocalDate()
                    );
                    return new Tuple2<>(key, pair._2());
                })
                .groupByKey()
                .flatMapValues(iterable -> {
                    ArrayList<Tuple2<Double, Long>> vals = new ArrayList<>();
                    iterable.forEach(vals::add);
                    ArrayList<Tuple2<Double, Long>> filledVals = new ArrayList<>();
                    // remove all NaN with the correct value
                    // NaN means in this context that in that hour there are no recorded update, so it's value did not change.
                    for (int i = 0; i < vals.size(); i++) {
                        double val = vals.get(i)._1();
                        long count = vals.get(i)._2();
                        if (i == 0) {
                            if (Double.isNaN(val)) {
                                val = 0.0;
                            }
                            filledVals.add(new Tuple2<>(val, count));
                        } else {
                            if (Double.isNaN(val)) {
                                double lastVal = filledVals.get(i - 1)._2();
                                filledVals.add(new Tuple2<>(lastVal, count));
                            }  else {
                                double lastVal = filledVals.get(i - 1)._2();
                                filledVals.add(new Tuple2<>(val, count));
                            }
                        }
                    }
                    return filledVals.iterator();
                }).groupByKey().flatMapValues(iterable -> {
                    ArrayList<Tuple2<Double, Long>> filledVals = new ArrayList<>();
                    iterable.forEach(filledVals::add);
                    ArrayList<Tuple2<Double, Long>> diff = new ArrayList<>();
                    for (int i = 1; i < filledVals.size(); i++) {
                        diff.add(new Tuple2<>(
                                filledVals.get(i)._1() - filledVals.get(i - 1)._1(),
                                filledVals.get(i)._2() + filledVals.get(i - 1)._2()
                        ));
                    }
                    return diff.iterator();
                });
        return filledValue;
    }




}
