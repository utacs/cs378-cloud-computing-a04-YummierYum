package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.Writable;
import java.io.IOException;

public class TaxiDriverEarnings implements Writable {
    private IntWritable numSeconds;
    private FloatWritable earnings;

    public TaxiDriverEarnings (int numSeconds, float earnings) {
        this.numSeconds = new IntWritable(numSeconds);
        this.earnings = new FloatWritable(earnings);
    }

    public TaxiDriverEarnings() {
        this.numSeconds = new IntWritable(0);
        this.earnings = new FloatWritable(0);
    }

    public float getEarningsRatio() {
        return this.earnings.get() / ((float) this.numSeconds.get() / 60);
    }

    public void write(DataOutput out) throws IOException {
        numSeconds.write(out);
        earnings.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        numSeconds.readFields(in);
        earnings.readFields(in);
    }

    public int getSeconds() {
        return this.numSeconds.get();
    }

    public float getEarnings() {
        return this.earnings.get();
    }

    public String toString() {
        return this.getEarningsRatio() + "";
    }
    
}
