import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import javax.json.*;

/**
 * It counts how many times each individual tweet is retweeted.
 */
public class CountRetweetsTweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     * The actual Mapper method
     *
     * @param key    The line number, useless.
     * @param value  The Json entity contained within the line.
     * @param output The id of the tweet and the number of times it was retweeted.
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        // The key is the character offset within the file of the start of the line, ignored.
        // The value is a line from the file.

        try {
            String line = value.toString();
            JsonObject tweet = Json.createReader(new StringReader(line)).readObject();
            //Tests if the tweet is deleted or not, deleted tweets have no hashtags.
            if (!tweet.containsKey("id_str") && !tweet.containsKey("deleted")) {
                System.out.println("Broken Json Entity");
            }
            if (tweet.containsKey("retweeted_status")) {
                JsonObject retweetedStatus = tweet.getJsonObject("retweeted_status");

                if (retweetedStatus.containsKey("retweet_count")) {
                    int retweetCount = retweetedStatus.getInt("retweet_count");

                    if (retweetedStatus.containsKey("id_str")) {
                        String idStr = retweetedStatus.getString("id_str");
                        output.write(new Text(idStr), new LongWritable(retweetCount));
                    } else {
                        System.out.println("no 'name' detected");
                    }
                } else {
                    System.out.println("no 'retweet_count' detected");
                }
            } else if (tweet.containsKey("retweet_count")) {
                int retweetCount = tweet.getInt("retweet_count");
                if (retweetCount > 0 && tweet.containsKey("id_str")) {
                    String idStr = tweet.getString("id_str");
                    output.write(new Text(idStr), new LongWritable(retweetCount));
                }
            }
        } catch (JsonException e) {
            System.out.println(e.getMessage());
        }
    }
}