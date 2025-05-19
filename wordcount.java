// Import necessary Java and Hadoop classes
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A simple Hadoop MapReduce program to count the number of occurrences of each word in a text file.
 */
public class WordCount {

    /**
     * Mapper class: takes a line of text and breaks it into words.
     * Emits each word with a count of 1.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        // Hadoop's wrapper for the integer 1
        private final static IntWritable one = new IntWritable(1);

        // Hadoop's wrapper for text
        private Text word = new Text();

        /**
         * This method is called once per line of input.
         * @param key     : The offset of the line (not used here).
         * @param value   : The actual line of text.
         * @param context : Used to emit (key, value) pairs.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Break the line into words using StringTokenizer
            StringTokenizer itr = new StringTokenizer(value.toString());

            // For each word, emit it with a count of 1
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken()); // Set the word
                context.write(word, one);  // Emit (word, 1)
            }
        }
    }

    /**
     * Reducer class: receives all values associated with a word and sums them up.
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // To store the final count
        private IntWritable result = new IntWritable();

        /**
         * This method is called once per key (word).
         * @param key     : The word.
         * @param values  : Iterable list of 1s.
         * @param context : Used to emit the final (word, count).
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            // Add up all the 1s for this word
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);              // Set the total count
            context.write(key, result);  // Emit (word, totalCount)
        }
    }

    /**
     * The main method configures and runs the job.
     */
    public static void main(String[] args) throws Exception {
        // Create a new Hadoop Configuration
        Configuration conf = new Configuration();

        // Define a new Job and give it a name
        Job job = Job.getInstance(conf, "word count");

        // Set the JAR file where this class is located
        job.setJarByClass(WordCount.class);

        // Set Mapper, Combiner, and Reducer classes
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);  // Optimization: Combiner reduces map output locally
        job.setReducerClass(IntSumReducer.class);

        // Set the types of output key and value (word, count)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths from command-line args
        FileInputFormat.addInputPath(job, new Path(args[0]));   // Input file path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path

        // Submit the job and wait until it finishes
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
