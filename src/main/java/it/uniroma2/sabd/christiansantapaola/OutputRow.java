package it.uniroma2.sabd.christiansantapaola;

public class OutputRow {
    String Date;
    String ID;
    String Count;
    String Average;
    String Max;
    String Min;

    public OutputRow(String id, String date, String average, String count, String max, String min) {
        Date = date;
        ID = id;
        Count = count;
        Average = average;
        Max = max;
        Min = min;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s", Date, ID, Count, Average, Max, Min);
    }
}
