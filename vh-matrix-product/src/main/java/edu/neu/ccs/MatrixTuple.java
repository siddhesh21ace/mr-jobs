package edu.neu.ccs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom writable implementation to present ipput matrix tuple
 */
public class MatrixTuple implements Writable {
    // Some data
    private Text name;
    private LongWritable rowIndex;
    private LongWritable columnIndex;
    private LongWritable value;

    MatrixTuple() {
        set(new Text(), new LongWritable(), new LongWritable(), new LongWritable());
    }

    MatrixTuple(MatrixTuple matrixTuple) {
        set(matrixTuple.getName(), matrixTuple.getRowIndex(), matrixTuple.getColumnIndex(), matrixTuple.getValue());
    }

    MatrixTuple(String name, String rowIndex, String columnIndex, String value) {
        this(name, Long.valueOf(rowIndex), Long.valueOf(columnIndex), Long.valueOf(value));
    }

    private MatrixTuple(String name, Long rowIndex, Long columnIndex, Long value) {
        set(new Text(name), new LongWritable(rowIndex), new LongWritable(columnIndex), new LongWritable(value));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        rowIndex.write(out);
        columnIndex.write(out);
        value.write(out);
    }

    public void set(Text name, LongWritable rIndex, LongWritable cIndex, LongWritable val) {
        this.name = name;
        this.rowIndex = rIndex;
        this.columnIndex = cIndex;
        this.value = val;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        rowIndex.readFields(in);
        columnIndex.readFields(in);
        value.readFields(in);
    }

    @Override
    public String toString() {
        return name.toString() + "\t" + rowIndex.toString() + "\t" + columnIndex.toString() + "\t" + value.toString();
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public LongWritable getRowIndex() {
        return rowIndex;
    }

    public void setRowIndex(LongWritable rowIndex) {
        this.rowIndex = rowIndex;
    }

    public LongWritable getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(LongWritable columnIndex) {
        this.columnIndex = columnIndex;
    }

    public LongWritable getValue() {
        return value;
    }

    public void setValue(LongWritable value) {
        this.value = value;
    }
}
