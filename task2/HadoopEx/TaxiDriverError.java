package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.Writable;
import java.io.IOException;

public class TaxiDriverError implements Writable {
    private IntWritable numFlights;
    private FloatWritable delay;

    public TaxiDriverError (IntWritable numFlights, FloatWritable delay) {
        this.numFlights = numFlights;
        this.delay = delay;
    }

    public TaxiDriverError() {
        this.numFlights = new IntWritable(0);
        this.delay = new FloatWritable(0);
    }

    public void addDelay(FloatWritable delay) {
        this.delay = new FloatWritable(this.delay.get() + delay.get());
        this.numFlights = new IntWritable(this.numFlights.get() + 1); 
    }

    public float getAvgDelay() {
        return this.delay.get() / this.numFlights.get();
    }   

    public void write(DataOutput out) throws IOException {
        numFlights.write(out);
        delay.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        numFlights.readFields(in);
        delay.readFields(in);
    }

    public FloatWritable get() {
        return delay;
    }

    public String toString() {
        return this.getAvgDelay() + "";
    }

    public int compareTo(Flights other) {
        float diff = this.getAvgDelay() - other.getAvgDelay();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }
    
}
