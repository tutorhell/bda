import java.io.IOException; 
import java.util.StringTokenizer; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
 
public class WeatherAnalysis { 
 
    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> { 
        public void map(Object key, Text value, Context context) throws IOException, 
InterruptedException { 
            StringTokenizer tokens = new StringTokenizer(value.toString(), "\t"); 
            String date = "", time = "", temp = ""; 
            float minTemp = Float.MAX_VALUE, maxTemp = Float.MIN_VALUE; 
            String minTime = "", maxTime = ""; 
 
            while (tokens.hasMoreTokens()) { 
                date = tokens.nextToken(); 
                time = tokens.nextToken(); 
                temp = tokens.nextToken(); 
                float currentTemp = Float.parseFloat(temp); 
 
                if (currentTemp < minTemp) { minTemp = currentTemp; minTime = time; } 
                if (currentTemp > maxTemp) { maxTemp = currentTemp; maxTime = time; } 
            } 
 
            context.write(new Text(date), new Text("MinTemp: " + minTemp + " Time: " + minTime + " 
MaxTemp: " + maxTemp + " Time: " + maxTime)); 
        } 
    } 
 
    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> { 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
InterruptedException { 
            String result = ""; 
            for (Text val : values) { 
                result = val.toString(); 
            } 
            context.write(key, new Text(result)); 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "Weather Analysis"); 
        job.setJarByClass(WeatherAnalysis.class); 
        job.setMapperClass(WeatherMapper.class); 
        job.setReducerClass(WeatherReducer.class); 
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(Text.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 
 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 