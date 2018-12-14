package edu.neu.ccs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static edu.neu.ccs.Constants.A;
import static edu.neu.ccs.Constants.B;

/**
 * Reducer implementation for step1 of col-by-row matrix multiplication
 */
public class ProductReducer extends Reducer<Text, MatrixTuple, IndexPair, LongWritable> {
    private IndexPair outkey = new IndexPair();
    private LongWritable outValue = new LongWritable(1);

    private List<MatrixTuple> aList = new ArrayList<>();
    private List<MatrixTuple> bList = new ArrayList<>();

    private static void populateNodes(Iterable<MatrixTuple> values,
                                      List<MatrixTuple> aList,
                                      List<MatrixTuple> bList,
                                      final Context context) {
        for (final MatrixTuple matrixTuple : values) {
            if (matrixTuple.getName().toString().equals(A)) {
                aList.add(WritableUtils.clone(matrixTuple, context.getConfiguration()));
            } else if (matrixTuple.getName().toString().equals(B)) {
                bList.add(WritableUtils.clone(matrixTuple, context.getConfiguration()));
            }
        }
    }

    /**
     * @param key     input key
     * @param values  corresponding values
     * @param context program context
     * @throws IOException          program error
     * @throws InterruptedException program error
     */
    @Override
    public void reduce(final Text key, final Iterable<MatrixTuple> values, final Context context)
            throws IOException, InterruptedException {

        aList.clear();
        bList.clear();

        populateNodes(values, aList, bList, context);

        if (!aList.isEmpty() && !bList.isEmpty()) {
            for (MatrixTuple a : aList) {
                for (MatrixTuple b : bList) {
                    outkey.set(a.getRowIndex(), b.getColumnIndex());
                    outValue.set(a.getValue().get() * b.getValue().get());

                    context.write(outkey, outValue);
                }
            }
        }
    }

}