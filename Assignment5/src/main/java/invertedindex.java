import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.HashMap;

public class invertedindex {

    public static class Map extends Mapper<LongWritable, Text, wordpair, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            HashMap<wordpair, IntWritable> map = new HashMap<wordpair, IntWritable>();
            /*Get the name of the file using context.getInputSplit()method*/
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            //Split the line in words
            String words[] = line.split(" ");
            for (String s : words) {
                wordpair wordpair = new wordpair(new Text(s), new Text(fileName));
                if (map.containsKey(wordpair)) {
                    map.put(wordpair, new IntWritable(map.get(wordpair).get() + 1));
                } else {
                    map.put(wordpair, new IntWritable(1));
                }
            }

            for (HashMap.Entry<wordpair, IntWritable> entry : map.entrySet()) {
                wordpair temp = entry.getKey();
                IntWritable val = entry.getValue();
                context.write(temp, val);
            }

        }
    }

    public static class IntSumReducer extends Reducer<wordpair, IntWritable, wordpair, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(wordpair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(invertedindex.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(IntSumReducer.class);
        //job.setPartitionerClass(WordPairPartitioner.class);
        job.setOutputKeyClass(wordpair.class);
        job.setNumReduceTasks(3); //0 turns off the reducer
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




}



















