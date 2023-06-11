package it.uniroma2.sabd.christiansantapaola;

import com.google.gson.Gson;

import java.io.Serializable;
import java.time.LocalDate;

public class Q2Out implements Serializable {
    //Tuple2<String, LocalDate>, Tuple2<Long, Tuple2<Double, Double>>
    private String ID;
    private LocalDate dayDate;
    private long count;
    private double mean;
    private double stdvar;


    public Q2Out(String ID, LocalDate dayDate, long count, double mean, double stdvar) {
        this.ID = ID;
        this.dayDate = dayDate;
        this.count = count;
        this.mean = mean;
        this.stdvar = stdvar;
    }

    public String getID() {
        return ID;
    }

    public LocalDate getDayDate() {
        return dayDate;
    }

    public long getCount() {
        return count;
    }

    public double getStdvar() {
        return stdvar;
    }

    public double getMean() {
        return mean;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public String toCSV() {
        return String.format("%s,%s,%d,%f,%f", ID, dayDate, count, mean, stdvar);
    }

}

