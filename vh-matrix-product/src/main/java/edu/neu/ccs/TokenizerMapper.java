package edu.neu.ccs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import static edu.neu.ccs.Constants.*;

/**
 * Mapper implementation for step 1 of col-by-row implementation
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, MatrixTuple> {
    private static final Logger LOG = LogManager.getLogger(VHMatrixProduct.class);
    private static final int LIMIT = 115000;

    private Text outkey = new Text();

    /**
     * @param key     input key
     * @param value   corresponding values
     * @param context program context
     * @throws IOException          program error
     * @throws InterruptedException program error
     */
    @Override
    public void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {

        String[] split = value.toString().split(SPACE_SEPARATOR);

        String row = split[0];
        String col = split[1];
        String val = split[2];

        if (Integer.parseInt(row) > LIMIT || Integer.parseInt(col) > LIMIT) {
            return;
        }

        outkey.set(col);
        context.write(outkey, new MatrixTuple(A, row, col, val));

        outkey.set(row);
        context.write(outkey, new MatrixTuple(B, row, col, val));

    }
}
