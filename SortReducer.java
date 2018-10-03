import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reduce method for the sorting function, applied to all map-reduce functions.
 */
public class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
    /**
     * The actual Reduce method
     *
     * @param key    The number of times the text occured.
     * @param values The different texts (tweets, ids, or users) associated with each count.
     * @param output Outputs all the texts concatenated,
     * @throws IOException
     * @throws InterruptedException
     */

    // The output of the reducer is a map from unique words to their total counts.
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

        // The key is the word.
        // The values are all the counts associated with that word (commonly one copy of '1' for each occurrence).
        StringBuilder temp = new StringBuilder();
        for (Text value : values) {
            temp.append(value + ",");
        }
        output.write(new Text(temp.substring(0, temp.length() - 1)), key);
    }
}
