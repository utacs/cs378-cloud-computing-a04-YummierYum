package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;

public class WordCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

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