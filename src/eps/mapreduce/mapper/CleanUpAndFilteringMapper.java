package eps.mapreduce.mapper;

    import eps.mapreduce.writabletypes.GpsPositionWritable;
    import eps.mapreduce.writabletypes.TaxiTripWritable;
    import org.apache.commons.math3.analysis.function.Exp;
    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class CleanUpAndFilteringMapper extends Mapper<Text, Text, Text, TaxiTripWritable>
{
    private TaxiTripWritable taxiTrip = new TaxiTripWritable();
    private float distance, time, velocity;

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String[] values = value.toString().split(",");

            // Compose members of TaxiTrip Writable object
            taxiTrip.setIdTrip(GetValue(key.toString()));
            taxiTrip.setIdTaxi(GetValue(values[3]));
            taxiTrip.setCallType(GetValue(values[0]));
            taxiTrip.setOriginCall(GetValue(values[1]));
            taxiTrip.setOriginStand(GetValue(values[2]));
            taxiTrip.setTimeStamp(new IntWritable(Integer.parseInt(GetValue(values[4]).toString())));
            taxiTrip.setDayType(GetValue(values[5]));
            taxiTrip.setMissingData(new BooleanWritable(Boolean.valueOf(GetValue(values[6]).toString())));
            try {
                taxiTrip.setPolyline(GetPolyline(value.toString()));
            } catch (Exception e) {

            }
            try {
                taxiTrip.setPositions(GetPositions(value.toString()));
            } catch (Exception e) {

            }

            // Compute input information
            distance = this.computeDistance(taxiTrip.getPositions());
            time = this.computeTime(taxiTrip.getPositions().length);
            velocity = this.computeVelocity(time, distance);

            taxiTrip.setDistance(new FloatWritable(distance));
            taxiTrip.setTripTime(new FloatWritable(time));
            taxiTrip.setVelocityAvg(new FloatWritable(velocity));

            // Write Context
            context.write(
                    this.taxiTrip.getIdTaxi(),
                    taxiTrip);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    // Get value from data of file
    private Text GetValue(String value) {
        value = CleanText(value);
        value = value.substring(value.indexOf(":")+1, value.length());
        return new Text(value);
    }

    // Parser GPS Latitude and Longitude positions
    private GpsPositionWritable[] GetPositions(String value) {
        value = CleanText(value);

        value = value.substring(value.indexOf("[[") + 1, value.indexOf("]]") + 1);
        String[] values = value.split("]");

        GpsPositionWritable[] gpsPositions = new GpsPositionWritable[values.length];

        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].replaceAll(",\\[", "").replaceAll("\\[", "");
            String[] longlat = values[i].split(",");

            GpsPositionWritable gpsPosition = new GpsPositionWritable(
                    Float.valueOf(longlat[0]),
                    Float.valueOf(longlat[1])
            );
            gpsPositions[i] = gpsPosition;
        }

        return gpsPositions;
    }

    // Parser GPS Latitude and Longitude in string
    private Text GetPolyline(String value) {
        StringBuilder positions = new StringBuilder();
        value = CleanText(value);

        value = value.substring(value.indexOf("[[") + 1, value.indexOf("]]") + 1);
        String[] values = value.split("]");

        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].replaceAll(",\\[", "").replaceAll("\\[", "");

            positions.append(values[i]);
            positions.append(i < values.length-1 ? ":": "");
        }
        //System.out.println("Polyline: " + positions.toString());
        return new Text(positions.toString());
    }

    // Clean Text message
    private String CleanText(String text) {
        text = text.replaceAll("(\r\n|\n\r|\r|\n)", "");
        text = text.replaceAll("\"", "");
        text = text.replaceAll(" ", "");
        text = text.replace("rows:", "");
        return text;
    }

    private Float computeDistance (GpsPositionWritable[] coordinates){

        float totalDistance = 0;
        for (int i = 0; i<coordinates.length-1; i++){

            float lt1 = coordinates[i].getLatitude();
            float lt2 = coordinates[i+1].getLatitude();
            float lg1 = coordinates[i].getLongitude();
            float lg2 = coordinates[i+1].getLongitude();

            float Lat1 = (float)((lt1*3.141592654)/180);
            float Lat2 = (float)((lt2*3.141592654)/180);

            float Long1 = (float)((lg1*3.141592654)/180);
            float Long2 = (float)((lg2*3.141592654)/180) ;

            float distance = (float)(6371.0 * Math.acos(Math.cos(Lat1) * Math.cos(Lat2) * Math.cos(Long2 - Long1) +
                    Math.sin(Lat1) * Math.sin(Lat2))
            );

            totalDistance += distance;
        }
        if (totalDistance >0 )
            return totalDistance;
        else
            return 0F;
    }

    private Float computeTime (int numberOfPositions){
        try {
            return (float) (numberOfPositions / 2.0 * 15 / 3600);
        } catch (Exception e) {
            return 0F;
        }
    }

    private Float computeVelocity(float time, float velocity) {
        try {
            return velocity / time;
        } catch (Exception e) {
            return 0F;
        }

    }
}
