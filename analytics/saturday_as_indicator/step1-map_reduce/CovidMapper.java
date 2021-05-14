import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CovidMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final int MISSING = 0;
    
    private static final String START_DATE = "2021-03-27";
    private static final String END_DATE = "2020-02-29";
    private boolean started = false;

    private int dateCounter = 0;

    // We use the date of every Saturday to represent 
    // the week before (and including) that day
    // e.g. "2020-03-07" represents the week from "2020-03-01" to "2020-03-07"
    private String currentDateIndicator = "2021-03-27";

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString();

        String[] words = line.split("\t");
        // handle weired file format exception
        if (words.length == 1) words = line.split("  ");

        String date = words[0];
        int newCases = Integer.parseInt(words[1]);
        
        // determine when the program should start and end
        if (START_DATE.equals(date)) started = true;
        if (END_DATE.equals(date)) started = false;

        if (!started) return;

        // if at the beginning of a period (a Saturaday), reset the indicator
        if (dateCounter == 0) currentDateIndicator = date;
        
        context.write(new Text(currentDateIndicator), new LongWritable(newCases));
        
        dateCounter++;
        dateCounter %= 7;
    }
}