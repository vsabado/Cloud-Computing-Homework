
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Writer;


public class BigramCount {
    private static int wordCount = 0;
    private static int bigramCount = 0;
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                System.out.println("Word before set: " + word);
                word.set(itr.nextToken());
                System.out.println("Word after set: " + word);
                context.write(word, one);
                wordCount++; //Added to keep a running count of how many words
                if(itr.hasMoreTokens()){
                    bigramCount++;
                }
            }
            word.set("1MapInvoked");
            context.write(word, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
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
        job.setJarByClass(BigramCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        boolean isComplete = job.waitForCompletion(true);

        Counters cn = job.getCounters();
        for (CounterGroup group : cn) {
            System.out.println("Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("Number of counters in this group: " + group.size());

            for (Counter counter : group) {
                System.out.println(" - " + counter.getDisplayName() + ": " + counter.getName() + ": " + counter.getValue());
            }

        }

        System.out.println("Is it done? " + isComplete);

        if (isComplete) {
            Counters pn = job.getCounters();
            System.out.println("Counter size: " + pn.countCounters());
            CounterGroup group = job.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter");
            System.out.println("Group Size: " + group.size());
            Counter counter = group.findCounter("MAP_INPUT_RECORDS");
            long val = counter.getValue();
            PrintStream ps = null;
            try {
                ps = new PrintStream("output/Result.txt");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            ps.println("Number of times map is invoked: ");
            ps.println(Integer.parseInt(String.valueOf(val)));
            ps.println("Total word count: ");
            ps.println(String.valueOf(wordCount));
            System.out.println("Total word count: " + wordCount);
            System.out.println("Total bigram count: " + bigramCount);
            System.out.println("Map invoked: " + String.valueOf(val));
//            System.exit(0);
        }
    }
}
