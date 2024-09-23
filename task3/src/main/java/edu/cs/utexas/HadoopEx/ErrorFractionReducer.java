package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorFractionReducer extends  Reducer<Text, TaxiDriverError, Text, TaxiDriverError> {

   public void reduce(Text text, Iterable<TaxiDriverError> values, Context context)
           throws IOException, InterruptedException {
	   
        int totalErrors = 0;
        int totalDrives = 0;
       
        for (TaxiDriverError value : values) {
            totalErrors += value.getErrors();
            totalDrives += value.getDrives();
        }
       
        context.write(text, new TaxiDriverError(totalErrors, totalDrives));
   }
}