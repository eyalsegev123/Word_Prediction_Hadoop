import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordCount{

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        //ngram format from Google Books:
        //ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            
            if (fields.length == 5) {
                // Extract n-gram and count data
                String ngram = fields[0]; // "w1 w2 w3"
                int matchCount = Integer.parseInt(fields[2]);
                
                // Emit n-gram
                word.set(ngram);
                context.write(word, new IntWritable(matchCount));
            }
        }
        
    }

    // yeled : 40 - send as it is
    // yeled tov : 50 - search yeled and send yeled tov yeled
    // yeled nehmad :90
    // yeled tov halah : 60
    // yeled nehmad halah : 70

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, Integer> currentWordGrams;
        private String currentFirstWord = null;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            String[] words = key.toString().split(" "); //Understand the split
            String firstWord = words[0];

            // Initialize or reset HashMap when first word changes
            if (currentFirstWord == null || !currentFirstWord.equals(firstWord)) {
                if (currentWordGrams != null) {
                    for(Map.Entry<String, Integer> entry : currentWordGrams.entrySet()) {
                        String ngram = entry.getKey();
                        String[] ngramWords = ngram.split(" "); 
                        int n = ngramWords.length;
                        String w1 = ngramWords[0];
                        
                        String sum = entry.getValue().toString();
                        if(n == 1){
                            context.write(new Text(ngram), new Text(ngram + ":" + sum));
                        }
                        else if (n == 2){
                            String w1Sum = currentWordGrams.get(w1).toString();
                            context.write(new Text(ngram), new Text(w1+ ":" + w1Sum + "\t" + ngram + ":" + sum));//value: טוב:50 --- TAB --- ״טוב הלך״:60 
                        }
                        else if (n == 3){
                            String w2 = ngramWords[1];
                            String w1Sum = currentWordGrams.get(w1).toString();
                            String w1w2Sum = currentWordGrams.get(w1 + " " + w2).toString();
                            context.write(new Text(ngram), new Text(w1 + ":" + w1Sum + "\t" + w1 + " " + w2 + ":" + w1w2Sum + "\t" + ngram + ":" + sum));
                            //value: טוב:50 --- TAB --- ״טוב ילד״:60 --- TAB --- ״טוב ילד נחמד״:70
                        }
                    }
                }
                currentFirstWord = firstWord;
                currentWordGrams = new HashMap<>();
            }
            //after this reduce we will have:
            //key:  
            //value: טוב:50 --- TAB --- ״טוב ילד״:60 --- TAB --- ״טוב ילד נחמד״:70

            // Sum up the counts for this n-gram
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            
            // Store in HashMap
            currentWordGrams.put(key.toString(), sum);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        TextOutputFormat.setOutputPath(job, new Path("s3://hashem-itbarach/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}