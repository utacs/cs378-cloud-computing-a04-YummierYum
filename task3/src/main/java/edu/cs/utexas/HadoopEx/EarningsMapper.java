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

public class EarningsMapper extends Mapper<Object, Text, Text, TaxiDriverEarnings> {


	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String [] lineSplit = value.toString().split(",");
        Text driverId = new Text(lineSplit[1]);
        

        Writer output = new BufferedWriter(new FileWriter("error_lines.txt", true));


        int duration = 0;
        float fare = 0;
        float surcharge = 0;
        float mta_tax = 0;
        float tip_amount = 0;
        float tolls_amount = 0;
        float total_amount = 0;
        boolean bad = false;


        try {
            duration = Integer.parseInt(lineSplit[4]);
            fare = Float.parseFloat(lineSplit[11]);
            surcharge = Float.parseFloat(lineSplit[12]);
            mta_tax = Float.parseFloat(lineSplit[13]);
            tip_amount = Float.parseFloat(lineSplit[14]);
            tolls_amount = Float.parseFloat(lineSplit[15]);
            total_amount = Float.parseFloat(lineSplit[16]);

        } catch (NumberFormatException e) {
            bad = true;
            output.append("BAD LINE (Can't parse values): " + value.toString() + "\n");
        }

        if(!bad) {
            if (Math.abs(fare + surcharge + mta_tax + tip_amount + tolls_amount - total_amount) > 0.001) {
                bad = true;
                output.append("BAD LINE (Total amount doesn't match): " + value.toString() + "\n");
            }

            if(duration <= 0) {
                bad = true;
                output.append("BAD LINE (invalid duration): " + value.toString() + "\n");
            }
        }

        if(!bad) {
            context.write(driverId, new TaxiDriverEarnings(duration, total_amount));
        }
        
        
        output.close();
        return;
	}
}