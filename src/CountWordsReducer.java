import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the supplied reducer method for counting words, little has changed.
 */

public class CountWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * The actual method for counting the words.
     *
     * @param key    The Word to be counted.
     * @param values How many times the word has occured as an iterable.
     * @param output Outputs the word, and how many times it has been read.
     * @throws IOException
     * @throws InterruptedException
     */

    // The output of the reducer is a map from unique words to their total counts.
    public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

        // The key is the word.
        // The values are all the counts associated with that word (commonly one copy of '1' for each occurrence).

        int sum = 0;
        for (LongWritable value : values) {
            long l = value.get();
            sum += l;
        }
        output.write(key, new LongWritable(sum));
    }
}
