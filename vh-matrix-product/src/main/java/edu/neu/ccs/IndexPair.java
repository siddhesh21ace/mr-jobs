package edu.neu.ccs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custo writable implementation to represent matrix input tuple's row and col
 */
public class IndexPair implements WritableComparable<IndexPair> {
    private LongWritable rowIndex;
    private LongWritable colIndex;

    public IndexPair() {
        set(new LongWritable(), new LongWritable());
    }

    public IndexPair(Long rowIndex, Long colIndex) {
        set(new LongWritable(rowIndex), new LongWritable(colIndex));
    }

    public IndexPair(LongWritable rowIndex, LongWritable colIndex) {
        set(rowIndex, colIndex);
    }

    public void set(LongWritable rowIndex, LongWritable colIndex) {
        this.rowIndex = rowIndex;
        this.colIndex = colIndex;
    }

    public LongWritable getrowIndex() {
        return rowIndex;
    }

    public LongWritable getcolIndex() {
        return colIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        rowIndex.write(out);
        colIndex.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rowIndex.readFields(in);
        colIndex.readFields(in);
    }

    @Override
    public int hashCode() {
        return rowIndex.hashCode() * 163 + colIndex.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IndexPair) {
            IndexPair ip = (IndexPair) o;
            return rowIndex.equals(ip.rowIndex) && colIndex.equals(ip.colIndex);
        }
        return false;
    }

    @Override
    public String toString() {
        return rowIndex + ":" + colIndex;
    }

    @Override
    public int compareTo(IndexPair ip) {
        int cmp = rowIndex.compareTo(ip.rowIndex);
        if (cmp != 0) {
            return cmp;
        }
        return colIndex.compareTo(ip.colIndex);
    }
}