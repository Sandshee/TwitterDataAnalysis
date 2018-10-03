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
public class MapperChoices {

    /**
     * The main method, which handles user interface, and calls the map and reduce methods.
     *
     * @param descisions The input and output path.
     * @param choice The integer chosen by the user.
     */
    public static void mapping(String[] descisions, int choice) {

        // This produces an output file in which each line contains a separate word followed by
        // the total number of occurrences of that word in all the input files.

        if (descisions.length != 2) {
            System.out.println("Error: inappropriate number of inputs.");
            return;
        }

        try {

            String input_path = descisions[0];
            String output_path = descisions[1];

            //Deletes any previous directory there, if possible.
            FileUtils.deleteDirectory(new File(output_path));

            //New configuration and job
            Configuration config = new Configuration();
            Job job = Job.getInstance(config, "MapperChoices");

            //Output and input paths
            FileInputFormat.setInputPaths(job, new Path(input_path));
            FileOutputFormat.setOutputPath(job, new Path(output_path + "-temp"));

            switch (choice) {
                //The user has selected the original, counting up how many times each hashtag was used.
                case (0): {
                    //Assigns the Mapper
                    job.setMapperClass(FindHashtagsMapper.class);

                    break;
                }

                case (1): {
                    //Assigns the Mapper
                    job.setMapperClass(CountRetweetsTweetMapper.class);

                    break;
                }

                //The user has selected to count how many times each user was retweeted.
                case (2): {
                    //Assigns the Mapper
                    job.setMapperClass(CountRetweetsMapper.class);

                    break;
                }

                default: {
                    System.out.println("An Error has occured.");
                    System.exit(0);
                }
            }

            // Specify output types produced by mapper (words with count of 1)
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // The output of the reducer is a map from unique words to their total counts.
            job.setReducerClass(CountWordsReducer.class);

            // Specify the output types produced by reducer (words with total counts)
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //The first search, to calculate the total number of retweets.
            job.waitForCompletion(true);

            //Adjusts the number of concurrently running map jobs locally.
            LocalJobRunner.setLocalMaxRunningMaps(job, 1);

            Job sortJob = Job.getInstance(config, "SortJob");

            //Sets the input path of the second job to the output path of the first.
            FileInputFormat.setInputPaths(sortJob, new Path(output_path + "-temp/part-r-00000"));
            FileOutputFormat.setOutputPath(sortJob, new Path(output_path));


            sortJob.setMapperClass(SortMapper.class);

            //Specifies the output types of the secon job.
            sortJob.setMapOutputKeyClass(LongWritable.class);
            sortJob.setMapOutputValueClass(Text.class);


            sortJob.setReducerClass(SortReducer.class);

            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(LongWritable.class);

            sortJob.setNumReduceTasks(1);

            sortJob.waitForCompletion(true);

            FileUtils.deleteDirectory(new File(output_path + "-temp"));

        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            System.out.println("Job done!");
        }
    }
}
