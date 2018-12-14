package multiplication;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Step2Mapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String entry[] = value.toString().split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        outputKey.set(entry[0]);
        outputValue.set(entry[1]);
        context.write(outputKey, outputValue);

    }

}



