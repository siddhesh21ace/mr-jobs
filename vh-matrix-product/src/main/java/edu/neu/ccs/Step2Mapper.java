package edu.neu.ccs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper for step 3 of col-by-row matrix implementation
 */
public class Step2Mapper extends Mapper<Object, Text, Text, LongWritable> {
    private static final LongWritable one = new LongWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            one.set(Long.parseLong(itr.nextToken()));
            context.write(word, one);
        }
    }
}