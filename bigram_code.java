import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bigram_code {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] key_value = value.toString().split("\t", 2);
            String line = key_value[1].toLowerCase().replaceAll("[^a-z]+", " ");
            String docID = key_value[0];
            StringTokenizer itr = new StringTokenizer(line);
            String[] my_bigrams = new String[] { "computer science", "information retrieval", "power politics",
                    "los angeles", "bruce willis" };
            String prev = itr.nextToken();
            while (itr.hasMoreTokens()) {
                String cur = itr.nextToken();
                String pair_of_words = prev + " " + cur;
                if (ArrayUtils.contains(my_bigrams, pair_of_words)) {
                    context.write(new Text(pair_of_words), new Text(docID));
                }
                prev = cur;
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> count_map = new HashMap<String, Integer>();
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
        Job job = Job.getInstance(conf, "Inverted Index for Bigrams");
        job.setJarByClass(bigram_code.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("./src/in/devdata"));
        FileOutputFormat.setOutputPath(job, new Path("./src/bigram_output_folder"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}