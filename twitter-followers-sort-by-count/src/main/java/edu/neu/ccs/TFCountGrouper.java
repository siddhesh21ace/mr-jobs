package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * Hadoop MapReduce implementation for sorting users based on no. of followers from Twitter dataset
 */
public class TFCountGrouper extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TFCountGrouper.class);
    private static final String COMMA_SEPARATOR = ",";

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new TFCountGrouper(), args);
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
        final Job job = Job.getInstance(conf, "Twitter Followers Count Grouper");
        job.setJarByClass(TFCountGrouper.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
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
    public static class SortMapper extends Mapper<Object, Text, Text, IntWritable> {

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
            String line = value.toString();
            context.write(new Text(line), new IntWritable(1));
        }
    }

    /**
     * Reducer implementation
     */
    public static class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * @param key     input key
         * @param values  corresponding values
         * @param context program context
         * @throws IOException          program error
         * @throws InterruptedException program error
         */
        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}