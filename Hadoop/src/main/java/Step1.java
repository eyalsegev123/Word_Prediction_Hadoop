import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Step1{

    public static class MapperClass1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private HashSet<String> stopWords = new HashSet<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // Configure AWS client using instance profile credentials (recommended when running on AWS infrastructure)
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                                    .withRegion("us-east-1") // Specify your bucket region
                                    .build();

            String bucketName = "hashem-itbarach"; // Your S3 bucket name
            String key = "heb-stopwords.txt"; // S3 object key for the stopwords file

            try {
                S3Object s3object = s3Client.getObject(bucketName, key);
                try (S3ObjectInputStream inputStream = s3object.getObjectContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopWords.add(line.trim());
                    }
                }
            } catch (Exception e) {
                // Handle exceptions properly in a production scenario
                System.err.println("Exception while reading stopwords from S3: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        //ngram format from Google Books:
        //ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            
            if (fields.length == 5) {
                // Extract n-gram and count data
                String ngram = fields[0]; // "w1 w2 w3"
                String [] words = ngram.split(" ");
                for(String word : words) {
                    if(stopWords.contains(word))
                        return;
                }
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

    public static class ReducerClass1 extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, Integer> currentWordGrams;
        private String currentFirstWord = null;
        protected long c0 = 0;


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            System.err.println("working on key: " + key.toString());

            String[] words = key.toString().split(" "); //Understand the split
            String firstWord = words[0];

            // Initialize or reset HashMap when first word changes
            if (currentFirstWord == null || !currentFirstWord.equals(firstWord)) {
                if (currentWordGrams != null) {
                    System.out.println("key has been changed from: " + currentFirstWord + " to:" + firstWord + ". passing hashMap.");
                    for(Map.Entry<String, Integer> entry : currentWordGrams.entrySet()) {
                        String ngram = entry.getKey();
                        System.out.println("current n-gram in hashMap: " + ngram);
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
            this.c0 += sum;
            
            // Store in HashMap
            currentWordGrams.put(key.toString(), sum);
            System.out.println("stored in hashMap:  <" + key.toString() + " , " + sum + ">");
        }

        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Configure AWS S3 client
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                                    .withRegion("us-east-1") // Specify your bucket region
                                    .build();
        
            String bucketName = "hashem-itbarach"; // Your S3 bucket name
            String key = "output/c0.txt"; // The path in the bucket where you want to upload
        
            try {
                // Convert c0 to a string to write to S3
                String c0Content = String.valueOf(c0);
        
                // Upload c0 content to S3
                s3Client.putObject(bucketName, key, c0Content);
        
                System.out.println("Successfully uploaded c0 to S3: " + c0);
            } catch (Exception e) {
                System.err.println("Error while uploading c0 to S3: " + e.getMessage());
                e.printStackTrace();
            }
        }
        

    }

    public static class PartitionerClass1 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    // public static class NgramComparator extends WritableComparator {
    //     protected NgramComparator() {
    //         super(Text.class, true);
    //     }
    
    //     @Override
    //     public int compare(WritableComparable w1, WritableComparable w2) {
    //         String[] ngram1 = ((Text) w1).toString().split(" ");
    //         String[] ngram2 = ((Text) w2).toString().split(" ");
            
    //         // Compare first words
    //         int cmp = ngram1[0].compareTo(ngram2[0]);
    //         if (cmp != 0) {
    //             return cmp;
    //         }
            
    //         // If first words are the same, compare lengths
    //         return Integer.compare(ngram1.length, ngram2.length);
    //     }
    // }


    
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1 - Word Counting");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass1.class);
        job.setPartitionerClass(PartitionerClass1.class);
        //job.setSortComparatorClass(NgramComparator.class);
        // job.setCombinerClass(ReducerClass1.class);
        job.setReducerClass(ReducerClass1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        TextOutputFormat.setOutputPath(job, new Path("s3://hashem-itbarach/output/step1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}