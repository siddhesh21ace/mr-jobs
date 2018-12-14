package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TriangleAmplifier extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TriangleAmplifier.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final Long MAX_NODE_ID = 20000L;

    public static class Step1Mapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitVals = value.toString().split(COMMA_SEPARATOR);

            if (splitVals.length != 2) {
                return;
            }

            String node1Val = splitVals[0];
            String node2Val = splitVals[1];

            if (Long.valueOf(node1Val) >= MAX_NODE_ID || Long.valueOf(node2Val) >= MAX_NODE_ID)
                return;

            outkey.set(node2Val);
            outvalue.set("F" + node1Val);

            context.write(outkey, outvalue);

            outkey.set(node1Val);
            outvalue.set("T" + node2Val);

            context.write(outkey, outvalue);
        }
    }

    public static class Step1Reducer extends Reducer<Text, Text, Text, Text> {
        private List<Text> fromList = new ArrayList<>();
        private List<Text> toList = new ArrayList<>();

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            fromList.clear();
            toList.clear();

            Step2Reducer.populateNodes(values, fromList, toList);

            if (!fromList.isEmpty() && !toList.isEmpty()) {
                for (Text A : fromList) {
                    outkey.set(A);
                    for (Text B : toList) {
                        outvalue.set("S" + B);
                        context.write(outkey, outvalue);
                    }
                }
            }
        }

    }

    public static class Step2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitVals = value.toString().split(COMMA_SEPARATOR);

            if (splitVals.length != 2) {
                return;
            }

            String node1Val = splitVals[0];
            String node2Val = splitVals[1];

            if (node2Val.charAt(0) == 'S') {
                outkey.set(node1Val);
                outvalue.set("T" + node2Val.substring(1));
            } else {
                outkey.set(node2Val);
                outvalue.set("F" + node1Val);
            }

            context.write(outkey, outvalue);
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {
        private List<Text> fromList = new ArrayList<>();
        private List<Text> toList = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) {
            fromList.clear();
            toList.clear();

            populateNodes(values, fromList, toList);

            if (!fromList.isEmpty() && !toList.isEmpty()) {
                fromList.sort(Comparator.comparing(Text::toString));

                for (Text B : toList) {
                    if (Collections.binarySearch(fromList, B) > -1) {
                        context.getCounter(CounterEnum.TRIANGLE_COUNTER).increment(1);
                    }
                }

            }
        }

        private static void populateNodes(Iterable<Text> values, List<Text> fromList, List<Text> toList) {
            for (Text text : values) {
                if (text.charAt(0) == 'F') {
                    fromList.add(new Text(text.toString().substring(1)));
                } else if (text.charAt(0) == 'T') {
                    toList.add(new Text(text.toString().substring(1)));
                }
            }
        }

    }

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new TriangleAmplifier(), args);
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
        final Job job = Job.getInstance(conf, "RS Join");
        job.setJarByClass(TriangleAmplifier.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        final Job job2 = Job.getInstance(conf, "RS Join Triangle Amplifier");
        job2.setJarByClass(TriangleAmplifier.class);

        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class);
        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class);

        job2.setMapperClass(Step2Mapper.class);
        job2.setReducerClass(Step2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        boolean isComplete = job2.waitForCompletion(true);

        Counters cn = job2.getCounters();
        Counter c1 = cn.findCounter(CounterEnum.TRIANGLE_COUNTER);
        logger.info("Triangle count:" + c1.getDisplayName() + ":" + c1.getValue() / 3);

        return isComplete ? 0 : 1;
    }
}


