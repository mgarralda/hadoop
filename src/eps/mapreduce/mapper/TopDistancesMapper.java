
package eps.mapreduce.mapper;

    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.io.NullWritable;

    import java.io.IOException;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.TreeMap;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class TopDistancesMapper extends Mapper<Object, Text, NullWritable, Text> {

    // Our output key and value Writables
    private TreeMap<Float, Text> topNRecordMap = new TreeMap<Float, Text>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Get Top N parameter
        org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
        int n = Integer.parseInt(conf.get("TopN"));

        String[] data = value.toString().split("\t");
        String[] distance = data[1].split(":");
        float sumDistance = Float.parseFloat((distance[1].replaceAll(",", ".").trim()));

        topNRecordMap.put(sumDistance, new Text(value));

        if (topNRecordMap.size() > n) {
            topNRecordMap.remove(topNRecordMap.firstKey()); // Remove all elements with this key
        }

        /*
        Note that in the map function we aren’t doing "context.write".
        We have to wait until the task is complete to output the results.

        The MapReduce framework provides us with a cleanup function that conveniently
        runs after the last map function runs. This is where the last of our code for the mapper will go.
        */
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Text t : topNRecordMap.values()) {
            context.write(NullWritable.get(), t);
        }
    }
}