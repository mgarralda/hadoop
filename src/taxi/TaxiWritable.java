package taxi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TaxiWritable implements  Writable {

    private Text trip_id,call_type, origin_call, origin_stand, day_type,taxi_id ,polyline;
    private BooleanWritable missing_data;
    private IntWritable timestamp;
    private FloatWritable distance,tripTime;


    public TaxiWritable(){

        this.trip_id = new Text();
        this.call_type = new Text();
        this.origin_call = new Text();
        this.origin_stand = new Text();
        this.taxi_id = new Text();
        this.timestamp = new IntWritable(0);
        this.day_type = new Text();
        this.polyline = new Text();
        this.missing_data = new BooleanWritable();
        this.distance = new FloatWritable(0);
        this.tripTime = new FloatWritable(0);


    }

    public TaxiWritable(String trip_id, String call_type, String origin_call, String origin_stand
            ,Text taxi_id, int timestamp, String day_type, String polyline, boolean missing_data){

        this.trip_id = new Text(trip_id);
        this.call_type = new Text(call_type);
        this.origin_call = new Text(origin_call);
        this.origin_stand = new Text(origin_stand);
        this.taxi_id = new Text(taxi_id);
        this.timestamp = new IntWritable(timestamp);
        this.day_type = new Text(day_type);
        this.polyline = new Text(polyline);
        this.missing_data = new BooleanWritable(missing_data);
    }


    public void readFields(DataInput arg0) throws IOException {

        trip_id.readFields(arg0);
        call_type.readFields(arg0);
        origin_call.readFields(arg0);
        origin_stand.readFields(arg0);
        taxi_id.readFields(arg0);
        timestamp.readFields(arg0);
        day_type.readFields(arg0);
        polyline.readFields(arg0);
        missing_data.readFields(arg0);
        distance.readFields(arg0);
        tripTime.readFields(arg0);
    }

    public void write(DataOutput arg0) throws IOException {

        trip_id.write(arg0);
        call_type.write(arg0);
        origin_call.write(arg0);
        origin_stand.write(arg0);
        taxi_id.write(arg0);
        timestamp.write(arg0);
        day_type.write(arg0);
        polyline.write(arg0);
        missing_data.write(arg0);
        distance.write(arg0);
        tripTime.write(arg0);
    }
    public FloatWritable getTripTime() {
        return tripTime;
    }

    public void setTripTime(FloatWritable tripTime) {
        this.tripTime = tripTime;
    }

    public FloatWritable getDistance() {
        return distance;
    }

    public void setDistance(FloatWritable distance) {
        this.distance = distance;
    }

    public Text getTrip_id() {
        return trip_id;
    }

    public void setTrip_id(Text trip_id) {
        this.trip_id = trip_id;
    }

    public Text getCall_type() {
        return call_type;
    }

    public void setCall_type(Text call_type) {
        this.call_type = call_type;
    }

    public Text getOrigin_call() {
        return origin_call;
    }

    public void setOrigin_call(Text origin_call) {
        this.origin_call = origin_call;
    }

    public Text getOrigin_stand() {
        return origin_stand;
    }

    public void setOrigin_stand(Text origin_stand) {
        this.origin_stand = origin_stand;
    }

    public Text getDay_type() {
        return day_type;
    }

    public void setDay_type(Text day_type) {
        this.day_type = day_type;
    }

    public Text getTaxi_id() {
        return taxi_id;
    }

    public void setTaxi_id(Text taxi_id) {
        this.taxi_id = taxi_id;
    }

    public Text getPolyline() {
        return polyline;
    }

    public void setPolyline(Text polyline) {
        this.polyline = polyline;
    }

    public BooleanWritable getMissing_data() {
        return missing_data;
    }

    public void setMissing_data(BooleanWritable missing_data) {
        this.missing_data = missing_data;
    }

    public IntWritable getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(IntWritable timestamp) {
        this.timestamp = timestamp;
    }

}

