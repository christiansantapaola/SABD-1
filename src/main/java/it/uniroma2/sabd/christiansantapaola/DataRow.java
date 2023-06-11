package it.uniroma2.sabd.christiansantapaola;



import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DataRow implements Serializable {
    String ID;
    String NationID;

    String Sectype;

    double Last;

    LocalDateTime LastUpdateTime;
    LocalDateTime LastTrading;

    public DataRow(String ID, String sectype, String Date, String Time,  String last, String lastTradingTime, String lastTradingDate) {
        String[] codes = ID.split("\\.");
        if (codes.length != 2) {
            this.ID = ID;
            this.NationID = "";
        } else {
            this.ID = codes[0];
            this.NationID = codes[1];
        }
        Sectype = sectype;
        try {
            Last = Double.parseDouble(last);
        } catch (NumberFormatException e) {
            Last = Double.NaN;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
        try {
            LastTrading = LocalDateTime.parse(lastTradingDate.trim() + " " + lastTradingTime.trim(), formatter);
        } catch (DateTimeParseException e) {
            LastTrading = null;
        }
        try {
            LastUpdateTime = LocalDateTime.parse(Date.trim() + " " + Time.trim(), formatter);
        }catch (DateTimeParseException e) {
            LastUpdateTime = null;
        }
    }

    public String getID() {
        return ID;
    }

    public String getNationID() {
        return NationID;
    }

    public String getSectype() {
        return Sectype;
    }

    public double getLast() {
        return Last;
    }

    public LocalDateTime getLastTrading() {
        return LastTrading;
    }

    @Override
    public String toString() {
        return "DataRow{" +
                "ID='" + ID + '\'' +
                ", NationID='" + NationID + '\'' +
                ", Sectype='" + Sectype + '\'' +
                ", Last=" + Last +
                ", LastUpdateTime='" + LastUpdateTime + '\'' +
                ", LastTrading='" + LastTrading + '\'' +
                '}';
    }

    public String getIDPlusLastTradingDate() {
        return getFullID() + "_" + LastTrading.withNano(0).withSecond(0).withMinute(0);
    }

    public String getIDPlusLastUpdateDate() {
        return getFullID() + "_" + LastUpdateTime.withNano(0).withSecond(0).withMinute(0);
    }


    public String getFullID() {
        return ID + "." + NationID;
    }

    public LocalDateTime getLastUpdateTime() {
        return LastUpdateTime;
    }
}
