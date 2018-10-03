import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import javax.json.*;

/**
 * The Mapper class to count how many times a person has been retweeted for a specific tweet.
 */

public class CountRetweetsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * The main method of the class.
     *
     * @param key    The line number in the class.
     * @param value  The Json entity about the tweet.
     * @param output Outputs the user was retweeted, and how many times they were retweeted for a specific tweet.
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

                    if (retweetedStatus.containsKey("user")) {
                        JsonObject user = retweetedStatus.getJsonObject("user");
                        if (user.containsKey("screen_name")) {
                            String screenName = user.getString("screen_name");
                            output.write(new Text(screenName), new LongWritable(retweetCount));
                        }
                    } else {
                        System.out.println("no 'name' detected");
                    }
                } else {
                    System.out.println("no 'retweet_count' detected");
                }
            }
        } catch (JsonException e) {
            System.out.println(e.getMessage());
        }
    }
}