package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorFractionMapper extends Mapper<Object, Text, Text, IntWritable> {


	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String [] line = value.toString().split(",");
        Writer output = new BufferedWriter(new FileWriter("error_lines.txt", true));

        //check pickup locations for zeroes
        if (Float.parseFloat(line[6]) == 0 || Float.parseFloat(line[7]) == 0) {
            output.append(value.toString() + "\n");
            int pickupTime = Integer.parseInt(line[2].split(" ")[1].split(":")[0]);
            context.write(new IntWritable(pickupTime + 1), new IntWritable(1));
            output.close();
            return;
        }
        //check dropoff locations for zeroes
        if (Float.parseFloat(line[8]) == 0 || Float.parseFloat(line[9]) == 0) {
            output.append(value.toString() + "/n");
            int dropoffTime = Integer.parseInt(line[3].split(" ")[1].split(":")[0]);
            context.write(new IntWritable(dropoffTime + 1), new IntWritable(1));
            output.close();
            return;
        } 
        output.close();
        return;
	}
}