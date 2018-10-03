import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;

/**
 * The Mapper class for finding hashtags in the tweet.
 */

public class FindHashtagsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * The actual method for doing so.
     *
     * @param key    The line in the file, fairly redundant.
     * @param value  The entire Json entity placed onto one line.
     * @param output The hashtag contained in the tweet, repeated if there are multiple.
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
            } else if (tweet.containsKey("entities")) {
                JsonObject entities = tweet.getJsonObject("entities");
                if (entities.containsKey("hashtags")) {
                    JsonArray hashtags = entities.getJsonArray("hashtags");
                    if (hashtags.size() > 0) {
                        for (int i = 0; i < hashtags.size(); i++) {
                            if (hashtags.getJsonObject(i).containsKey("text")) {
                                String hashtag = hashtags.getJsonObject(i).getString("text");
                                output.write(new Text(hashtag), new LongWritable(1));
                            }
                        }
                    }
                }
            }
        } catch (JsonException e) {
            System.out.println(e.getMessage());
        }
    }
}
