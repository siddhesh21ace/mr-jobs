package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Hadoop MapReduce implementation for
 */
public class ApproximateQuantiles extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ApproximateQuantiles.class);
    private static final String COMMA_SEPARATOR = ",";

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Three arguments required:\n<input-dir> <output-dir> <percentage>");
        }

        try {
            ToolRunner.run(new ApproximateQuantiles(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    /**
     * @param args runner arguments
     * @return job waiting statis
     * @throws Exception error during execution
     */
    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "K Good Centers");
        job.setJarByClass(ApproximateQuantiles.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        jobConf.set("filter_percentage", args[2]);

        job.setMapperClass(QuantileMapper.class);
        job.setReducerClass(QuantileReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper implementation
     */
    public static class QuantileMapper extends Mapper<Object, Text, NullWritable, IntWritable> {
        private Random rands = new Random();
        private Double percentage;

        @Override
        protected void setup(Context context) {
            String strPercentage = context.getConfiguration().get("filter_percentage");
            percentage = Double.parseDouble(strPercentage) / 100.0;
        }

        /**
         * @param key     input key
         * @param value   corresponding values
         * @param context program context
         * @throws IOException          program error
         * @throws InterruptedException program error
         */
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            int followerCount = Integer.parseInt(value.toString());

            if (rands.nextDouble() < percentage) {
                context.write(NullWritable.get(), new IntWritable(followerCount));
            }
        }
    }

    /**
     * Reducer implementation
     */
    public static class QuantileReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        /**
         * @param key     input key
         * @param values  corresponding values
         * @param context program context
         * @throws IOException          program error
         * @throws InterruptedException program error
         */
        @Override
        public void reduce(final NullWritable key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {

            int k = Integer.parseInt(context.getConfiguration().get("k"));
            String startConfig = context.getConfiguration().get("start_config");

            TreeSet<Integer> targetCollection = new TreeSet<>();

            for (IntWritable val : values) {
                targetCollection.add(val.get());
            }

            int incr;

            if (startConfig.equals("G")) {
                incr = (targetCollection.size() / (k - 1)) - 1;
            } else {
                incr = 25;
            }

            Iterator<Integer> iterator = targetCollection.iterator();

            int count = 0;
            int kCount = 0;

            while (iterator.hasNext()) {
                int val = iterator.next();
                if (count % incr == 0) {  // pick every ith element and emit
                    context.write(NullWritable.get(), new IntWritable(val));
                    kCount++;
                }
                count++;
                if (kCount == k) break;
            }

//            int first = targetCollection.first();
//            int last = targetCollection.last();
//
//            int range = (last - first)/3;
//
//            context.write(NullWritable.get(), new IntWritable(first));
//            context.write(NullWritable.get(), new IntWritable(first + range));
//            context.write(NullWritable.get(), new IntWritable(first + range * 2));
//            context.write(NullWritable.get(), new IntWritable(last));


        }

    }

}