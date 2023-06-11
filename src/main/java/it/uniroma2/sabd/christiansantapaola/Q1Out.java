package it.uniroma2.sabd.christiansantapaola;

import com.google.gson.Gson;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Q1Out implements Serializable {
    private String ID;
    private LocalDateTime time;
    private long count;
    private double avg;
    private double max;
    private double min;

    public Q1Out(String ID, LocalDateTime time, long count, double avg, double max, double min) {
        this.ID = ID;
        this.time = time;
        this.count = count;
        this.avg = avg;
        this.max = max;
        this.min = min;
    }

    public String getID() {
        return ID;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public long getCount() {
        return count;
    }

    public double getAvg() {
        return avg;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public String toCSV() {
        return String.format("%s,%s,%d,%f,%f,%f", ID, time, count, avg, max, min);
    }
}
