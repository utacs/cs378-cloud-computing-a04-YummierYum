package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarningsReducer extends  Reducer<Text, TaxiDriverEarnings, Text, TaxiDriverEarnings> {

   public void reduce(Text text, Iterable<TaxiDriverEarnings> values, Context context)
           throws IOException, InterruptedException {
	   
        float totalEarnings = 0;
        int duration = 0;
       
        for (TaxiDriverEarnings value : values) {
            totalEarnings += value.getEarnings();
            duration += value.getSeconds();
        }
       
        context.write(text, new TaxiDriverEarnings(duration, totalEarnings));
   }
}