package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

enum Error {
    SSE
}

/**
 * K-Means MapReduce implementation
 */
public class DistKMeans extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(DistKMeans.class);
    private static final int MAX_ITERATIONS = 10;
    private static final double EPSILON = 10;
    private static final String COMMA_SEPARATOR = ",";

    public static class KMeansMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {

        private List<Double> centroids = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException {
            BufferedReader rdr = null;

            try {
                URI[] files = context.getCacheFiles();
                logger.info("Files are" + Arrays.toString(files));

                if (files == null || files.length == 0) {
                    logger.info("File not found");
                    throw new FileNotFoundException("Edge information is not set in DistributedCache");
                }

                // Read all files in the DistributedCache
                for (URI u : files) {
                    String filename;

                    int lastindex = u.toString().lastIndexOf('/');
                    if (lastindex != -1) {
                        filename = u.toString().substring(lastindex + 1);
                    } else {
                        filename = u.toString();
                    }

                    rdr = new BufferedReader(new FileReader(filename));

                    String line;

                    while ((line = rdr.readLine()) != null) {
                        centroids.add(Double.parseDouble(line));
                    }
                }
            } catch (Exception e) {
                logger.info("Error occured while creating cache: " + e.getMessage());
            } finally {
                centroids.forEach(logger::warn);
                if (rdr != null) {
                    rdr.close();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            double point = Double.parseDouble(line);

            double currMin = Double.MAX_VALUE;
            double closestCenter = centroids.get(0);

            for (double c : centroids) {
                double dist = Math.abs(c - point);
                if (dist < currMin) {
                    closestCenter = c;
                    currMin = dist;
                }
            }

            context.write(new DoubleWritable(closestCenter), new DoubleWritable(point));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (double c : centroids) {
                context.write(new DoubleWritable(c), new DoubleWritable(-1));
            }
        }

    }

    public static class KMeansReducer extends Reducer<DoubleWritable, DoubleWritable, NullWritable, DoubleWritable> {
        @Override
        public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double newCenter;
            double sum = 0;
            double sse = 0;
            double centroid = key.get();

            int count = 0;
            boolean isOrphan = false;

            for (DoubleWritable value : values) {
                double d = value.get();
                if (d == -1) {
                    isOrphan = true;
                    continue;
                }
                sum = sum + d;
                sse = sse + Math.pow(centroid - d, 2);
                count++;
            }

            long gsse = (long) (context.getCounter(Error.SSE).getValue() + sse * 1000);
            context.getCounter(Error.SSE).setValue(gsse);

            newCenter = (count == 0 || (isOrphan && sum == 0)) ? centroid : sum / count;
            context.write(NullWritable.get(), new DoubleWritable(newCenter));
        }

    }

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 5) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new DistKMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        int code;

        // job for finding initial k centroids

        final Job job1 = Job.getInstance(conf, "K Good Centers");
        job1.setJarByClass(DistKMeans.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        jobConf.set("filter_percentage", args[2]);
        jobConf.set("k", args[3]);
        jobConf.set("start_config", args[4]);

        job1.setMapperClass(ApproximateQuantiles.QuantileMapper.class);
        job1.setReducerClass(ApproximateQuantiles.QuantileReducer.class);

        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setOutputFormatClass(TextOutputFormat.class);

        code = job1.waitForCompletion(true) ? 0 : 1;

        int i = 0;
        boolean isCompleted = false;

        String input = args[0];

        double sse = 0.0;

        while (!isCompleted && i < MAX_ITERATIONS) {
            String centroids;
            String output;

            if (i == 0) {
                centroids = args[1];
            } else {
                centroids = args[1] + "-" + (i - 1);
            }

            output = args[1] + "-" + i;

            final Job job = Job.getInstance(conf, "Distributed K Means");
            job.setJarByClass(DistKMeans.class);

            FileSystem fs = FileSystem.get(new URI(centroids), conf);
            FileStatus[] statuses = fs.listStatus(new Path(centroids));

            for (FileStatus status : statuses) {
                logger.error("Cache:" + status.getPath().toUri());
                job.addCacheFile(status.getPath().toUri());
            }

            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

            code = job.waitForCompletion(true) ? 0 : 1;

            Counters cn = job.getCounters();
            double newSse = cn.findCounter(Error.SSE).getValue() / 1000f;

            isCompleted = sse == newSse || Math.abs(sse - newSse) < EPSILON;

            logger.error("SSE after iteration " + i + " : " + newSse);
            sse = newSse;
            i++;
        }

        return code;
    }


}
