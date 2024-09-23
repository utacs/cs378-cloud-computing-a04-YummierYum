package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.Writable;
import java.io.IOException;

public class TaxiDriverError implements Writable, Comparable<TaxiDriverError> {
    private IntWritable numErrors;
    private IntWritable numDrives;

    public TaxiDriverError (int numErrors, int numDrives) {
        this.numErrors = new IntWritable(numErrors);
        this.numDrives = new IntWritable(numDrives);
    }



    public TaxiDriverError() {
        this.numErrors = new IntWritable(0);
        this.numDrives = new IntWritable(0);
    }

    public void addError() {
        this.numErrors.set(this.numErrors.get() + 1);
        this.numDrives.set(this.numDrives.get() + 1);
    }

    public void addDrive() {
        this.numDrives.set(this.numDrives.get() + 1);
    }

    public float getErrorRatio() {
        return (float) this.numErrors.get() / this.numDrives.get();
    }   

    public void write(DataOutput out) throws IOException {
        numDrives.write(out);
        numErrors.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        numDrives.readFields(in);
        numErrors.readFields(in);
    }

    public int getErrors() {
        return this.numErrors.get();
    }

    public int getDrives() {
        return this.numDrives.get();
    }

    public String toString() {
        return this.getErrorRatio() + "";
    }

    public int compareTo(TaxiDriverError other) {
        float diff = this.getErrorRatio() - other.getErrorRatio();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }
    
}
