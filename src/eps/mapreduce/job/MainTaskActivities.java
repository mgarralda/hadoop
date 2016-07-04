package eps.mapreduce.job;

    import eps.mapreduce.inputfileformat.TaxiTripInputFormat;
    import eps.mapreduce.mapper.CleanUpAndFilteringMapper;
    import eps.mapreduce.mapper.TopDistancesMapper;
    import eps.mapreduce.reducer.ComputeStatisticsReducer;
    import eps.mapreduce.reducer.TopDistancesReducer;

    import eps.mapreduce.writabletypes.TaxiTripWritable;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.conf.Configured;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.io.NullWritable;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionHelper;
    import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionMapper;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
    import org.apache.hadoop.util.GenericOptionsParser;
    import org.apache.hadoop.util.Tool;
    import org.apache.hadoop.util.ToolRunner;
    import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
    import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
public class MainTaskActivities extends Configured implements Tool {

    //private static final String inputPath = "D:/hadoop/_in/tweets1.json";
    private static String inputPath = "D:/hadoop/_in/taxis.json.txt";
    private static String outputPath = "D:/hadoop/_out/";

    @Override
    public int run(String[] args) throws Exception {

        // Generate config and get parameters
        Configuration conf = getConf();
        // Get arguments from command line arguments
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        /* Join all mapreduce's jobs in a single application that receives three parameters
       · Input file
       · Ouput dir
       · N: Top distances topic value
        if (args.length != 3) {
            System.err.printf("Usage: %s <input dir> <output dir> <N Top>\n",
                    getClass().getSimpleName());
            return -1;
        }
        */

        conf.set("TopN", "10"); //conf.set("TopN", args[2].toString());
        conf.set(FieldSelectionHelper.DATA_FIELD_SEPERATOR, ",");
        conf.set(FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "0:1,2,3,4,5,6,7,8-");
        conf.set(FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC, "0:1,2,3,4,5,6,7,8-");

        // 0. Field Selection Job
        Job job1 = Job.getInstance(conf);
        job1.setJobName("Field Taxi Trip Selection, CleanUp and Filtering");
        job1.setJarByClass(MainTaskActivities.class);
        //job1.setInputFormatClass(TextInputFormat.class);
        job1.setInputFormatClass(TaxiTripInputFormat.class);
        //job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setReducerClass(ComputeStatisticsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "_1"));

        ControlledJob cJob1 = new ControlledJob(conf);
        cJob1.setJob(job1);

        // Chain Mappers
        Configuration fieldSelectionConf = new Configuration(false);
        ChainMapper.addMapper(
                job1,
                FieldSelectionMapper.class,
                Text.class, Text.class,
                Text.class, Text.class,
                fieldSelectionConf);

        Configuration cleanUAndFilteringConf = new Configuration(false);
        ChainMapper.addMapper(
                job1,
                CleanUpAndFilteringMapper.class,
                Text.class, Text.class,
                Text.class, TaxiTripWritable.class,
                cleanUAndFilteringConf);

        // 2. The Top N pattern Job configuration
        Job job2 = Job.getInstance(conf);
        job2.setJobName("Top N Distances");
        job2.setJarByClass(MainTaskActivities.class);
        job2.setMapperClass(TopDistancesMapper.class);
        job2.setReducerClass(TopDistancesReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1); // Important Single Reducer Task
        FileInputFormat.addInputPath(job2, new Path(outputPath + "_1/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "_2"));
        ControlledJob cJob2 = new ControlledJob(conf);
        cJob2.setJob(job2);

        // Control and dependecy execution of Jobs
        JobControl jobctrl = new JobControl("jobctrl");
        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        cJob2.addDependingJob(cJob1);

        Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
        jobRunnerThread.start();

        while (!jobctrl.allFinished()) {
            System.out.println("Jobs still running...");
            Thread.sleep(2000);
        }

        System.out.println("Jobs done");
        jobctrl.stop();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MainTaskActivities(), args);
        System.exit(exitCode);
    }
}