import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] slot = line.split(",");
        String message = "Total number of records: ";
        if (slot.length > 1){ // this gets rid of the date and blank rows
                context.write(new Text(message), new IntWritable(1));
         }

    }

}
