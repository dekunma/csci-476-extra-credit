import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
 
public class TextComparator extends WritableComparator {
    
	public TextComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text ta = (Text) a;
		Text tb = (Text) b;
		return -ta.compareTo(tb);
	}
}