package eps.mapreduce.reducer;

    import eps.mapreduce.writabletypes.TaxiTripWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Reducer;

    import java.io.IOException;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class ComputeStatisticsReducer extends Reducer<Text, TaxiTripWritable, Text, Text>
{
    private float SumDistance, MaxDistance, AvgDistance, MaxVelocity, MaxTripTime, AvgVelocity, AvgTripTime;
    private int NumberOfTrips;
    private String idTaxiTrip;

    @Override
    public void reduce(Text key, Iterable<TaxiTripWritable> values,
                       Context context) throws IOException, InterruptedException
    {
        idTaxiTrip = "IDTAXI: " + key.toString();

        this.resetValues();

        for (TaxiTripWritable val : values) {
            NumberOfTrips++;
            SumDistance += Float.parseFloat(val.getDistance().toString());
            AvgVelocity += Float.parseFloat(val.getVelocityAvg().toString());
            AvgTripTime += Float.parseFloat(val.getTripTime().toString());

            // Calculate Maximum Values
            if (MaxDistance < Float.parseFloat(val.getDistance().toString()))
                MaxDistance = Float.parseFloat(val.getDistance().toString());

            if (MaxVelocity < Float.parseFloat(val.getVelocityAvg().toString()))
                MaxVelocity = Float.parseFloat(val.getVelocityAvg().toString());

            if (MaxTripTime < Float.parseFloat(val.getTripTime().toString()))
                MaxTripTime = Float.parseFloat(val.getTripTime().toString());
        }

        // Calculate Averages
        AvgTripTime = AvgTripTime/NumberOfTrips;
        AvgDistance = SumDistance/NumberOfTrips;
        AvgVelocity = AvgVelocity/NumberOfTrips;

        // Write Context
        context.write(new Text(idTaxiTrip), printStatistics());
    }

    private Text printStatistics() {

        return new Text(
                "DistanceTOT: " + String.format("%.2f", SumDistance)   + "\t" +
                "DistanceMAX: " + String.format("%.2f", MaxDistance)   + "\t" +
                //"DistanceMIN: " + String.format("%.2f", MinDistance)   + "\t" +
                "DistanceAVG: " + String.format("%.2f", AvgDistance)   + "\t" +
                "VelocityMAX: " + String.format("%.2f", MaxVelocity)   + "\t" +
                "VelocityAVG: " + String.format("%.2f", AvgVelocity)   + "\t" +
                "TripTimeMAX: " + String.format("%.2f", MaxTripTime)   + "\t" +
                //"TripTimeMIN: " + String.format("%.2f", MinTripTime)   + "\t" +
                "TripTimeAVG: " + String.format("%.2f", AvgTripTime)   + "\t" +
                "NumberTrips: " + String.valueOf(NumberOfTrips)
        );
    }

    private void resetValues() {
        NumberOfTrips = 0;
        SumDistance = 0;
        AvgVelocity = 0;
        AvgTripTime = 0;
        MaxDistance = 0;
        MaxVelocity = 0;
        MaxTripTime = 0;
        AvgTripTime = 0;
        AvgDistance = 0;
        AvgVelocity = 0;
    }

}
