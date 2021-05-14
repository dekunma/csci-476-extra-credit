import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CovidMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final int MISSING = 0;
    
    private static final String START_DATE = "2020-03-03";
    private static final String END_DATE = "2021-03-26";
    private boolean started = false;

    private int dateCounter = 0;
    private String currentStartDate = "2020-03-02";

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // ignore Saturday and Sunday
        // since the stock dataset does not consider the weekends
        if (dateCounter >= 5) {
            dateCounter++;
            dateCounter %= 7;
            return;
        }

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

        // if at the beginning of a week, reset the "currentStartDate"
        if (dateCounter == 0) currentStartDate = date;
        
        context.write(new Text(currentStartDate), new LongWritable(newCases));
        
        dateCounter++;
        dateCounter %= 7;
    }
}