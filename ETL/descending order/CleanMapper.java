import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Double;

public class CleanMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final int MISSING = 0;
    private static final int COUNTRY_INDEX = 2;
    private static final int DATE_INDEX = 3;
    private static final int NEW_CASES_INDEX = 5;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(",");
        long new_cases = 0;

        try {
            new_cases = (long) Double.parseDouble(words[NEW_CASES_INDEX]);
        }
        catch(NumberFormatException e) { // bad record
            new_cases = MISSING;
        }
        
        // ignore the header && non-US records
        if (!"date".equals(words[DATE_INDEX]) && "United States".equals(words[COUNTRY_INDEX]))
            context.write(new Text(words[DATE_INDEX]), new LongWritable(new_cases));
    }
}