package eps.mapreduce.writabletypes;

    import java.io.DataInput;
    import java.io.DataOutput;
    import java.io.IOException;

    import org.apache.hadoop.io.*;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class TaxiTripWritable implements WritableComparable<TaxiTripWritable>
{
    /*
    FILE ORGINAL DATA
    "TRIP_ID" : "1372636858620000589",
            "CALL_TYPE" : "C", -> Investigar qué singifica
            "ORIGIN_CALL" : null,
            "ORIGIN_STAND" : null,
            "TAXI_ID" : "20000589",
            "TIMESTAMP" : "1372636858",
            "DAY_TYPE" : "A", --> Investigar qué significa
            "MISSING_DATA" : "False",
            "POLYLINE" : "
     ADDED DATA
            Distance, TripTime, VelocityAvg
    */

    // Members declaration
    private Text IdTrip, IdTaxi, CallType, OriginCall, OriginStand, DayType, Polyline;
    private BooleanWritable MissingData;
    private IntWritable TimeStamp;
    private LongWritable TotalTrips;
    private FloatWritable Distance, TripTime, VelocityAvg;
    private GpsPositionWritable[] Positions;
    private ArrayWritable GpsPositionsArrayWritable = new ArrayWritable(GpsPositionWritable.class);

    // It is need a empty Constructor if there is a constructor with parameters
    public TaxiTripWritable(){

        this.IdTrip = new Text();
        this.IdTaxi = new Text();
        this.CallType = new Text();
        this.OriginCall = new Text();
        this.OriginStand = new Text();
        this.TimeStamp = new IntWritable(0);
        this.DayType = new Text();
        this.Positions = new GpsPositionWritable[0];
        this.MissingData = new BooleanWritable();
        this.Polyline = new Text();
        this.Distance = new FloatWritable(0);
        this.TripTime = new FloatWritable(0);
        this.VelocityAvg = new FloatWritable(0);
        this.TotalTrips = new LongWritable(0);
        this.GpsPositionsArrayWritable.set(Positions);
        /*
        this.MaxDistance = new FloatWritable(0);
        this.MinDistance = new FloatWritable(0);
        this.MaxVelocity = new FloatWritable(0);
        this.MaxTripTime = new FloatWritable(0);
        this.MinTripTime = new FloatWritable(0);
        this.AvgTripTime = new FloatWritable(0);
        */
    }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput arg0) throws IOException {
        try {
            this.IdTrip.readFields(arg0);
            this.IdTaxi.readFields(arg0);
            this.CallType.readFields(arg0);
            this.OriginCall.readFields(arg0);
            this.OriginStand.readFields(arg0);
            //this.GpsPositionsArrayWritable.readFields(arg0);
            //this.Polyline.readFields(arg0);
            this.TimeStamp.readFields(arg0);
            this.DayType.readFields(arg0);
            this.MissingData.readFields(arg0);
            this.Distance.readFields(arg0);
            this.TripTime.readFields(arg0);
            this.VelocityAvg.readFields(arg0);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput arg0) throws IOException {
        this.IdTrip.write(arg0);
        this.IdTaxi.write(arg0);
        this.CallType.write(arg0);
        this.OriginCall.write(arg0);
        this.OriginStand.write(arg0);
        this.TimeStamp.write(arg0);
        this.DayType.write(arg0);
        this.MissingData.write(arg0);
        //this.Polyline.write(arg0);
        //this.GpsPositionsArrayWritable.write(arg0);
        this.Distance.write(arg0);
        this.TripTime.write(arg0);
        this.VelocityAvg.write(arg0);
    }

    @Override
    public int compareTo(TaxiTripWritable o) {
        return IdTrip.compareTo(o.getIdTrip());
        //return 1;
    }

    // It is necessary if OutPutFormat job is TexOuputFormat.class
    public String toString() {
        return
                this.IdTrip.toString()      + "\t" +
                this.CallType.toString()    + "\t" +
                this.OriginCall.toString()  + "\t" +
                this.OriginStand.toString() + "\t" +
                this.TimeStamp.toString()   + "\t" +
                this.DayType.toString()     + "\t" +
                this.MissingData.toString() + "\t" +
                this.Distance.toString()    + "\t" +
                this.TripTime.toString()    + "\t" +
                this.VelocityAvg.toString();
    }


    // Getters and Setters
    public Text getIdTrip() { return this.IdTrip; } //return java.util.UUID.randomUUID().toString(); }
    public void setIdTrip(Text idTrip) { this.IdTrip = idTrip; }

    public Text getIdTaxi() { return this.IdTaxi; }
    public void setIdTaxi(Text idTaxi) { this.IdTaxi = idTaxi; }

    public FloatWritable getTripTime() { return this.TripTime; }
    public void setTripTime(FloatWritable tripTime) { this.TripTime = tripTime; }

    public FloatWritable getDistance() { return this.Distance; }
    public void setDistance(FloatWritable distance) { this.Distance = distance; }

    public FloatWritable getVelocityAvg() { return this.VelocityAvg; }
    public void setVelocityAvg(FloatWritable velocityAvg) { this.VelocityAvg = velocityAvg; }

    public Text getCallType() { return this.CallType; }
    public void setCallType(Text callType) { this.CallType = callType; }

    public Text getOriginCall() { return this.OriginCall; }
    public void setOriginCall(Text originCall) { this.OriginCall = originCall; }

    public Text getOriginStand() { return this.OriginStand; }
    public void setOriginStand(Text originStand) { this.OriginStand = originStand; }

    public Text getDayType() { return this.DayType; }
    public void setDayType(Text dayType) { this.DayType = dayType; }

    public BooleanWritable getMissingData() { return this.MissingData; }
    public void setMissingData(BooleanWritable missingData) { this.MissingData = missingData; }

    public Text getPolyline() { return this.Polyline; }
    public void setPolyline(Text polyline) { this.Polyline = polyline; }

    public IntWritable getTimeStamp() { return this.TimeStamp; }
    public void setTimeStamp(IntWritable timestamp) { this.TimeStamp = timestamp; }

    public LongWritable getTotalTrips() { return this.TotalTrips; }
    public void setTotalTrips(LongWritable totalTrips) { this.TotalTrips = totalTrips; }

    public Text getIdTripTaxi() { return new Text(this.IdTrip.toString() + "," + this.IdTaxi.toString()); }

    public GpsPositionWritable[] getPositions() {
        return (GpsPositionWritable[])this.GpsPositionsArrayWritable.get();
    }
    public void setPositions(GpsPositionWritable[] positions) {
        this.Positions = positions;
        this.GpsPositionsArrayWritable.set(positions);
    }
}
