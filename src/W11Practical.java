import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * The main file, containing the map and reduce files.
 */
public class W11Practical {

    /**
     * The main method, which handles user interface, and calls the map and reduce methods.
     *
     * @param args The input and output path
     */
    public static void main(String[] args) {

        // This produces an output file in which each line contains a separate word followed by
        // the total number of occurrences of that word in all the input files.

        if (args.length < 2) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_path>");
            return;
        }

        try {

            String input_path = args[0];
            String output_path = args[1];

            //Deletes any previous directory there, if possible. Commented out so that you don't accidentally delete a file.
            //FileUtils.deleteDirectory(new File(output_path));

            //New configuration and job
            Configuration config = new Configuration();
            Job job = Job.getInstance(config, "W11Practical");

            //Output and input paths
            FileInputFormat.setInputPaths(job, new Path(input_path));
            FileOutputFormat.setOutputPath(job, new Path(output_path));

            //Assigns the mapper
            job.setMapperClass(FindHashtagsMapper.class);

            // Specify output types produced by mapper (words with count of 1)
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // The output of the reducer is a map from unique words to their total counts.
            job.setReducerClass(CountWordsReducer.class);

            // Specify the output types produced by reducer (words with total counts)
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //Adjusts the number of concurrently running map jobs locally.
            LocalJobRunner.setLocalMaxRunningMaps(job, 1);

            //Adjusts the number of concurrently running reduce jobs locally
            LocalJobRunner.setLocalMaxRunningReduces(job, 1);
            //Both these make no immediate difference to the runtime.

            job.setNumReduceTasks(1);

            job.waitForCompletion(true);

        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
