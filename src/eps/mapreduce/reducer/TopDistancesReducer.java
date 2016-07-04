package eps.mapreduce.reducer;

    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.io.NullWritable;

    import java.io.IOException;
    import java.util.TreeMap;


/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class TopDistancesReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Float, Text> topNRecordMap = new TreeMap<Float, Text>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        // Get Top N parameter
        org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
        int n = Integer.parseInt(conf.get("TopN"));

        for (Text value : values) {

            String[] data = value.toString().split("\t");
            String[] distance = data[1].split(":");
            float sumDistance = Float.parseFloat((distance[1].replaceAll(",", ".").trim()));

            topNRecordMap.put(sumDistance, new Text(value));

            if (topNRecordMap.size() > n) {
                topNRecordMap.remove(topNRecordMap.firstKey());
            }
        }
        int i = 0;
        for (Text t : topNRecordMap.descendingMap().values()) {
            i++;
            context.write(NullWritable.get(), FormatOutPut(t, i));
        }
    }

    private Text FormatOutPut(Text t, int topN)
    {
        String[] inPut = t.toString().split("\t");
        String outPut =  "Top:" + Integer.toString(topN) + "\t" + inPut[0]  + "\t" + inPut[1];

        return new Text(outPut);
    }
}
