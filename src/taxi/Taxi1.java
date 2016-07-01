package taxi;

import java.io.* ;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class     Taxi1
{
    public static class TaxiMapper
            extends Mapper<Object, Text, Text, TaxiWritable>
    {

        private TaxiWritable taxiWritable = new TaxiWritable();

        private Text word = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] part = value.toString().split("\\{");
            for (String part2 : part){

                String[] part3 = part2.trim().split(":");


                if (part3[0].startsWith("\"POLYLINE\"")){
                    taxiWritable.setDistance(new FloatWritable(computeDistance(part3[1].substring(2,part3[1].length()-3))));
                    taxiWritable.setTripTime(new FloatWritable(computeTime(part3[1].substring(2,part3[1].length()-3))));
                    context.write(taxiWritable.getTaxi_id(),taxiWritable);

                }
                if (part3[0].startsWith("\"TRIP_ID\""))
                    taxiWritable.setTrip_id(new Text(part3[1]));
                if (part3[0].startsWith("\"TAXI_ID\""))
                    taxiWritable.setTaxi_id(new Text(part3[1].substring(2, part3[1].length()-2)));


            }


        }
        private float computeDistance (String coordinates){

            String [] coord = coordinates.split(",");

            float totalDistance = 0;
            for (int i = 0; i< coord.length-3; i=i+4){

                float lt1 = Float.parseFloat(coord[i].substring(2,coord[i].length()).trim());
                float lt2 = Float.parseFloat(coord[i+2].substring(2,coord[i+2].length()).trim());
                float lg1 = Float.parseFloat(coord[i+1].substring(0,coord[i+1].length()-1).trim());
                float lg2 = Float.parseFloat(coord[i+3].substring(0,coord[i+3].length()-1).trim());


                float Lat1=(float) ((lt1*3.141592654)/180);
                float Lat2=(float) ((lt2*3.141592654)/180);

                float Long1=(float) ((lg1*3.141592654)/180);
                float Long2=(float) ((lg2*3.141592654)/180) ;
                float distance= (float) (6371.0 * Math.acos(Math.cos(Lat1) * Math.cos(Lat2) * Math.cos(Long2 - Long1) + Math.sin(Lat1) * Math.sin(Lat2)));
                totalDistance += distance;;
            }
            if (totalDistance>0)
                return totalDistance;
            else
                return 0;


        }


        private float computeTime (String coordinates){

            String [] coord = coordinates.split(",");
            return (float) (coord.length/2.0*15/3600);

        }

    }



    public static class TaxiReducer

            extends Reducer<Text,TaxiWritable,Text,Text> {

        private FloatWritable result = new FloatWritable();
        private Text k = new Text();

        public void reduce(Text key, Iterable<TaxiWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            float sum = 0;
            int trips = 0;
            float time = 0;
            StringBuffer text =new StringBuffer();
            float velocity = 0;

            for (TaxiWritable val : values) {
                float dist = Float.parseFloat(val.getDistance().toString());
                time += Float.parseFloat(val.getTripTime().toString());
                sum += dist;
                trips++;



            }
            velocity = (float)sum/time;
            text.append("TotalDistance: "+sum+" km    NumberOfTrips: "+trips+ " \t totalTime "+time+" h    \t AverageVelocity: "+velocity+ " km/h");
            Text finalResult = new Text(text.toString());
            k.set("TaxiId: "+key);
            context.write(k, finalResult);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Taxi");
        job.setJarByClass(Taxi1.class);
        job.setMapperClass(TaxiMapper.class);
        job.setReducerClass(TaxiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TaxiWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




}

