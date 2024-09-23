package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;

public class ErrorFractionMapper extends Mapper<Object, Text, Text, TaxiDriverError> {


	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String [] line = value.toString().split(",");
        Text taxiId = new Text(line[0]);
        Writer output = new BufferedWriter(new FileWriter("error_lines.txt", true));

        // error lines
        if (Float.parseFloat(line[6]) == 0 || Float.parseFloat(line[7]) == 0 || Float.parseFloat(line[8]) == 0 || Float.parseFloat(line[9]) == 0) {
            output.append(value.toString() + "\n");

            int pickupTime = Integer.parseInt(line[2].split(" ")[1].split(":")[0]);
            context.write(taxiId, new TaxiDriverError(1, 1));
            
        
        } else {
            context.write(taxiId, new TaxiDriverError(0, 1));
        }

        
        output.close();
        return;
	}
}