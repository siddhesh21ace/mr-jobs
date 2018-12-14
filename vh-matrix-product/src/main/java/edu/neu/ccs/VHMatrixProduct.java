package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static edu.neu.ccs.Constants.SPACE_SEPARATOR;

/**
 * Hadoop MapReduce implementation for Parallel Matrix Multiplication: Column-by-Row
 */
public class VHMatrixProduct extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(VHMatrixProduct.class);

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Three arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new VHMatrixProduct(), args);
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
        final Job job = Job.getInstance(conf, "VH Product");
        job.setJarByClass(VHMatrixProduct.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", SPACE_SEPARATOR);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(ProductReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixTuple.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        final Job job2 = Job.getInstance(conf, "VH Sum");
        job2.setJarByClass(VHMatrixProduct.class);

        job2.setMapperClass(Step2Mapper.class);
        job2.setCombinerClass(SumReducer.class);
        job2.setReducerClass(SumReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        boolean isComplete = job2.waitForCompletion(true);

        return isComplete ? 0 : 1;
    }
}