package multiplication;

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

/**
 * H-V Partitioning using 1- Bucket Random Algorithm
 */

public class MatrixMultiplication extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        int exitcode = 0;
        final Configuration conf = getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        // A and B determine the granualirity of 1-Bucket Random Algorithm
        conf.set("a", "15");
        conf.set("b", "15");
        final Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(MatrixMultiplication.class);
        String input = args[0];
        String output = args[1];

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixTuple.class);
        job.setOutputKeyClass(IndexPair.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        exitcode = job.waitForCompletion(true) ? 0 : 1;


        final Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(MatrixMultiplication.class);

        String outputFinal = args[2];

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Step2Mapper.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);
        FileInputFormat.addInputPath(job2, new Path(output));
        FileOutputFormat.setOutputPath(job2, new Path(outputFinal));


        exitcode = job2.waitForCompletion(true) ? 0 : 1;

        return exitcode;
    }


}
