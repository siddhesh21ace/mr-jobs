package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
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
import java.nio.ByteBuffer;

/**
 * Hadoop MapReduce implementation for sorting users based on no. of followers from Twitter dataset
 */
public class TFCountSorter extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TFCountSorter.class);
    private static final String COMMA_SEPARATOR = ",";

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new TFCountSorter(), args);
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
        final Job job = Job.getInstance(conf, "Twitter Followers Count Sorter");
        job.setJarByClass(TFCountSorter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(IntComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper implementation
     */
    public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

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
            String[] tokens = line.split(COMMA_SEPARATOR); // This is the delimiter between
            context.write(new IntWritable(Integer.parseInt(tokens[1])),
                    new IntWritable(Integer.parseInt(tokens[0])));
        }
    }

    public static class IntComparator extends WritableComparator {
        public IntComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }

    /**
     * Reducer implementation
     */
    public static class SortReducer extends Reducer<IntWritable, IntWritable, NullWritable, IntWritable> {
        /**
         * @param key     input key
         * @param values  corresponding values
         * @param context program context
         * @throws IOException          program error
         * @throws InterruptedException program error
         */
        @Override
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {

            for (IntWritable ignored : values) context.write(NullWritable.get(), key);
        }
    }

}