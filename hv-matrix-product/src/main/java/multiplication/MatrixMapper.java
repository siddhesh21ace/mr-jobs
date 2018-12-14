package multiplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class MatrixMapper extends Mapper<Object, Text, Text, MatrixTuple> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String entry[] = value.toString().split("\\s");
        Configuration conf = context.getConfiguration();

        int a = Integer.parseInt(conf.get("a"));
        int b = Integer.parseInt(conf.get("b"));


        Random random1 = new Random();
        int row = random1.nextInt(a);
        int col = random1.nextInt(b);
        int k;
        Text outputKey = new Text();
        MatrixTuple outputValue = new MatrixTuple();

        // Partitioning using 1-Bucket Random Algorithm

        for (k = row * b; k <= row * b + b - 1; k++) {
            outputKey.set(new Text(Integer.toString(k)));
            outputValue.set(new Text("A"), new LongWritable(Long.parseLong(entry[0])),
                    new LongWritable(Long.parseLong(entry[1])),
                    new LongWritable(Long.parseLong(entry[2])));


            context.write(outputKey, outputValue);
        }

        for (k = col; k <= (a - 1) * b + col; k = k + b) {
            outputKey.set(new Text(Integer.toString(k)));
            outputValue.set(new Text("B"), new LongWritable(Long.parseLong(entry[0])),
                    new LongWritable(Long.parseLong(entry[1])),
                    new LongWritable(Long.parseLong(entry[2])));
            context.write(outputKey, outputValue);
        }

    }
}
