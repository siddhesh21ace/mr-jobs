package multiplication;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (Text value : values) {
            sum = sum + Long.parseLong(value.toString());
        }
        Text outvalue = new Text(Long.toString(sum));
        context.write(key, outvalue);

    }
}