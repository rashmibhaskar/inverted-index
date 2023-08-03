import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class unigram_code{
   public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
      private Text word = new Text();
      private Text docID = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String[] key_value = value.toString().split("\t", 2);
         String line = key_value[1].toLowerCase().replaceAll("[^a-z]+", " ");

         docID.set(key_value[0]);
         StringTokenizer itr = new StringTokenizer(line);
         while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, docID);
         }
      }
   }

   public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         HashMap<String, Integer> count_map = new HashMap<>();
         for (Text val : values) {
            count_map.put(val.toString(), count_map.getOrDefault(val.toString(), 0) + 1);
         }
         String s = "";
         for (String docID : count_map.keySet()) {
            s += docID + ":" + count_map.get(docID) + " ";
         }
         context.write(key, new Text(s));
      }
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Inverted Index for Unigrams");
      job.setJarByClass(unigram_code.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path("./src/in/fulldata"));
      FileOutputFormat.setOutputPath(job, new Path("./src/unigram_output_folder"));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}