import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The Mapper method of sorting, simply switches around keys and values.
 */

public class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    /**
     * The actual Mapper method.
     *
     * @param key    The line number for the input file.
     * @param value  both the frequency and the text associated, needs to be split up.
     * @param output outputs the keys and values, but reversed.
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        // The key is the character offset within the file of the start of the line, ignored.
        // The value is a line from the file.

        String[] values = value.toString().split("\t");

        int count = Integer.parseInt(values[1]);

        output.write(new LongWritable(count), new Text(values[0]));
    }
}
