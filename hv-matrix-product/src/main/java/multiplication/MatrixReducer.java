package multiplication;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MatrixReducer extends Reducer<Text, MatrixTuple, IndexPair, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<MatrixTuple> values, Context context) throws IOException, InterruptedException {

        List<MatrixTuple> listA = new LinkedList<MatrixTuple>();
        List<MatrixTuple> listB = new LinkedList<MatrixTuple>();
        IndexPair outputKey = new IndexPair();
        LongWritable outputValue = new LongWritable();

        for (MatrixTuple value : values) {

            if (value.getName().toString().equals("A")) {
                listA.add(WritableUtils.clone(value, context.getConfiguration()));
            } else {
                listB.add(WritableUtils.clone(value, context.getConfiguration()));
            }
        }

        for (MatrixTuple tupleA : listA) {
            for (MatrixTuple tupleB : listB) {
                if (tupleA.getColumnIndex().get() == tupleB.getRowIndex().get()) {
                    outputKey.set(tupleA.getRowIndex(), tupleB.getColumnIndex());
                    long v = tupleA.getValue().get() * tupleB.getValue().get();
                    outputValue.set(v);
                    context.write(outputKey, outputValue);
                }
            }
        }

    }
}
