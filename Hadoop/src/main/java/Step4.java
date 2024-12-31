import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;



public class Step4 {
    public static class MapperClass4 extends Mapper<Text, Text, Text, Text> {
        private long c0;
        protected void setup(Context context) throws IOException, InterruptedException {
            //Read c0 from s3Client 
             AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                                .withRegion("us-east-1") // Specify your bucket region
                                .build();

            String bucketName = "hashem-itbarach"; // Your S3 bucket name
            String key = "output/c0.txt"; // The path to the file in the bucket

            try {
                // Retrieve the object from S3
                S3Object s3Object = s3Client.getObject(bucketName, key);

                // Read the content of the file
                try (S3ObjectInputStream inputStream = s3Object.getObjectContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

                    // Parse the content to retrieve c0
                    String line = reader.readLine();
                    if (line != null) {
                        c0 = Long.parseLong(line.trim());
                        System.out.println("Successfully loaded c0 from S3: " + c0);
                    } else {
                        System.err.println("c0 file is empty.");
                    }
                }
            } catch (Exception e) {
                System.err.println("Error while reading c0 from S3: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        //ngram format from Google Books:
        //ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(Text textKey, Text textValue, Context context) throws IOException, InterruptedException {
            
            String[] values = textValue.toString().split("\t"); // Split by tab
            for(String value : values) {
                int indexOfColon = value.lastIndexOf(":");
                value = value.substring(0, indexOfColon);
            }
            long N1 = Integer.parseInt(values[0]); //String N1
            long N2 = Integer.parseInt(values[2]); //String N2
            long N3 = Integer.parseInt(values[5]); //String N3
            long C1 = Integer.parseInt(values[1]); //String C1
            long C2 = Integer.parseInt(values[4]); //String C2
            
            double K2 = (Math.log(N2+1) + 1) / (Math.log(N2+1) + 2);
            double K3 = (Math.log(N3+1) + 1) / (Math.log(N3+1) + 2);
            
            //Calculate the probability
            double probabilty = K3*(N3/C2) + (1-K3)*K2*(N2/C1) + (1-K3)*(1-K2)*(N1/c0);

            // Create a new output key and value
            String words = textKey.toString(); // Split key by space 
            Text outputKey = new Text(words + " " + probabilty); // Copy the key
            Text outputValue = new Text(""); // Concatenate probability with original data
            
            // Write the key-value pair to the context
            context.write(outputKey, outputValue);
        }
        
    }


    public static class ReducerClass4 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }        
    }            

    public static class PartitionerClass4 extends Partitioner<Text, Text> { //Get partition wasnt done today
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split(" ");
            int hash = (parts[0] + parts[1]).hashCode(); // Combine `w1` and `w2`
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class Step4Comparator extends WritableComparator {
        protected Step4Comparator() {
            super(Text.class, true);
        }
    
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String[] ngram1 = ((Text) w1).toString().split(" ");
            String[] ngram2 = ((Text) w2).toString().split(" ");
            
            // Compare first words
            int cmp = ngram1[0].compareTo(ngram2[0]);
            if (cmp != 0) {
                return cmp;
            }
            // Compare second words
            cmp = ngram1[1].compareTo(ngram2[1]);
            if (cmp != 0) {
                return cmp;
            }

            // Compare probabilities (reverse order for descending)
            // Assuming probabilities are the last element in the array and are parseable as doubles
            double prob1 = Double.parseDouble(ngram1[ngram1.length - 1]);
            double prob2 = Double.parseDouble(ngram2[ngram2.length - 1]);

            return prob1 < prob2 ? 1 : (prob1 > prob2 ? -1 : 0);
            
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

        // Step 1: Initialize Configuration
        Configuration conf = new Configuration();

        // Step 2: Configure the Job
        Job job = Job.getInstance(conf, "Step 4 - Processing with c0");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass4.class);
        job.setPartitionerClass(PartitionerClass4.class);
        job.setSortComparatorClass(Step4Comparator.class);
        job.setReducerClass(ReducerClass4.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Step 3: Set Input/Output Paths
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://hashem-itbarach/output/step3"));
        TextOutputFormat.setOutputPath(job, new Path("s3://hashem-itbarach/output/step4"));

        // Step 4: Run the Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
     
}