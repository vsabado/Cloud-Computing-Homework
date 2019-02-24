import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class relativefreq1 {

    public static class WordPairPartitioner extends Partitioner<wordpair1, IntWritable> {

        @Override
        public int getPartition(wordpair1 wordpair1, IntWritable intWritable, int numPartitions) {
            int check = wordpair1.getWord().hashCode() % numPartitions;
            int result = check < 0 ? check * -1 : check;
            return result;
        }
    }

    public static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, wordpair1, IntWritable> {
        private wordpair1 wordpair1 = new wordpair1();
        private IntWritable ONE = new IntWritable(1);
        private IntWritable totalCount = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            int neighbors = context.getConfiguration().getInt("neighbors", 1);
            String[] tokens = value.toString().split("\\s+");          // split the words using spaces
            if (tokens.length > 1) {
                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].replaceAll("\\W+", "");   // remove all non-word characters

                    if (tokens[i].equals("")) {
                        continue;
                    }

                    wordpair1.setWord(tokens[i]);

                    int start = (i - neighbors < 0) ? 0 : i - neighbors;
                    int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                    for (int j = start; j <= end; j++) {
                        if (j == i) continue;
                        wordpair1.setNeighbor(tokens[j].replaceAll("\\W", ""));
                        context.write(wordpair1, ONE);
                    }
                    wordpair1.setNeighbor("*");
                    totalCount.set(end - start);
                    context.write(wordpair1, totalCount);
                }
            }
        }
    }

    public static class PairsRelativeOccurrenceReducer extends Reducer<wordpair1, IntWritable, wordpair1, DoubleWritable> {
        private DoubleWritable totalCount = new DoubleWritable();
        private DoubleWritable relativeCount = new DoubleWritable();
        private Text currentWord = new Text("NOT_SET");
        private Text flag = new Text("*");

        @Override
        protected void reduce(wordpair1 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (key.getNeighbor().equals(flag)) {          //start a new section of word pairs with a new left word
                if (key.getWord().equals(currentWord)) {   //keep adding the counts of (word, *).
                    //In fact, this will not happen because ...
                    totalCount.set(totalCount.get() + getTotalCount(values));
                } else {                                   //reset the count when encounting (word, *) for the first time
                    currentWord.set(key.getWord());
                    totalCount.set(0);
                    totalCount.set(getTotalCount(values));
                }
            } else {                                       //calculate the
                int count = getTotalCount(values);
                relativeCount.set((double) count / totalCount.get());
                context.write(key, relativeCount);
            }
        }

        private int getTotalCount(Iterable<IntWritable> values) {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            return count;
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
        job.setJarByClass(relativefreq1.class);
        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setOutputKeyClass(wordpair1.class);
        job.setNumReduceTasks(3); //0 turns off the reducer
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}