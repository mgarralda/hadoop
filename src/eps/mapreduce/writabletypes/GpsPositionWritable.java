package eps.mapreduce.writabletypes;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class GpsPositionWritable implements WritableComparable<GpsPositionWritable> {

    private Float Longitude, Latitude;

    // It is need a empty Constructor if there is a constructor with parameters
    public GpsPositionWritable() {

        this.Longitude = 0F;
        this.Latitude = 0F;
    }

    // Constructor to file original data
    public GpsPositionWritable(Float longitude, Float latitude) {
        this.Longitude = longitude;
        this.Latitude = latitude;
    }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput arg0) throws IOException {
        try
        {
            new FloatWritable(this.Longitude).readFields(arg0);
            new FloatWritable(this.Latitude).readFields(arg0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput arg0) throws IOException {
        try {
            new FloatWritable(this.Longitude).write(arg0);
            new FloatWritable(this.Latitude).write(arg0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public int compareTo(GpsPositionWritable o) {
        //return IdTrip.compareTo(o.getIdTrip());
        return 1;
    }


    // Getters and Setters
    public Float getLatitude() {
        return this.Latitude;
    } //return java.util.UUID.randomUUID().toString(); }
    public void setLatitude(Float latitude) {
        this.Latitude = latitude;
    }

    public Float getLongitude() {
        return this.Longitude;
    }
    public void setLongitude(Float longitude) {
        this.Longitude = longitude;
    }

}

