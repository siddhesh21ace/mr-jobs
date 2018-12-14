package edu.neu.ccs;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;

public class TriangleAmplifier extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TriangleAmplifier.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final Long MAX_NODE_ID = 50000L;

    public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, Set<String>> nodeMapping = new HashMap<>();

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

                    rdr =  new BufferedReader(new FileReader(filename));

                    String line;
                    while ((line = rdr.readLine()) != null) {
                        String[] splitVals = line.split(COMMA_SEPARATOR);

                        if (splitVals.length != 2) {
                            return;
                        }

                        String node1Val = splitVals[0];
                        String node2Val = splitVals[1];

                        if (Long.valueOf(node1Val) >= MAX_NODE_ID || Long.valueOf(node2Val) >= MAX_NODE_ID)
                            continue;

                        nodeMapping.computeIfAbsent(node2Val, node -> new HashSet<>()).add(node1Val);
                    }
                }
            } catch (Exception e) {
                logger.info("Error occured while creating cache: " + e.getMessage());
            } finally {
                if (rdr != null) {
                    rdr.close();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) {
            String[] splitVals = value.toString().split(COMMA_SEPARATOR);

            if (splitVals.length != 2) {
                return;
            }

            String node1Val = splitVals[0];
            String node2Val = splitVals[1];

            if (Long.valueOf(node1Val) >= MAX_NODE_ID || Long.valueOf(node2Val) >= MAX_NODE_ID)
                return;

            Set<String> nodes = nodeMapping.get(node1Val);

            if (CollectionUtils.isNotEmpty(nodes)) {
                // increment counter
                for (String node : nodes) {
                    Set<String> nodes1 = nodeMapping.get(node);
                    if (CollectionUtils.isNotEmpty(nodes1) && nodes1.contains(node2Val)) {
                        context.getCounter(CounterEnum.TRIANGLE_COUNTER).increment(1);
                    }
                }
            }
        }
    }

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
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
        final Job job = Job.getInstance(conf, "Rep Join Analysis");
        job.setJarByClass(TriangleAmplifier.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        job.setMapperClass(EdgeMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[0] + "/edges.csv"));

        boolean isComplete = job.waitForCompletion(true);

        Counters cn = job.getCounters();
        Counter c1 = cn.findCounter(CounterEnum.TRIANGLE_COUNTER);

        logger.info("Path 2 edge count:" + c1.getDisplayName() + ":" + c1.getValue() / 3);

        return isComplete ? 0 : 1;

    }
}


