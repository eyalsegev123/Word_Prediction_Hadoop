import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step2{
    
    public static class MapperClass2 extends Mapper<Text, Text, Text, Text> {
        private Text word = new Text();

        //ngram format from Google Books:
        //ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(Text textKey, Text textValue, Context context) throws IOException, InterruptedException {
            String key = textKey.toString();
            String[] words = key.split(" "); // Split by tab
            
            if (words.length == 3) {
                // Extract n-gram and count data
                String twoGram = words[1] + " " + words[2]; // "w1 w2 w3"
                // Emit 2-gram
                word.set(twoGram);
                context.write(word, new Text(textValue));
            }
            else 
                context.write(textKey, textValue);
        }
        
    }

    // yeled : 40 - send as it is
    // yeled tov : 50 - search yeled and send yeled tov yeled
    // yeled nehmad :90
    // yeled tov halah : 60
    // yeled nehmad halah : 70

    public static class ReducerClass2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");

            if(words.length == 1) {
                String valueOf1gram = "";
                for(Text value : values)
                    valueOf1gram += value.toString();
                context.write(key, new Text(valueOf1gram));
            }

            else if(words.length != 2){
                System.out.println("error: got a 3gram");
                return;
            }

            else { //Length is 2 
                String original2GramValue = null;
                for(Text value: values) {
                    if(value.toString().split("\t").length == 2) {
                        original2GramValue = value.toString();
                        break; 
                    }        
                }
                
                for (Text value : values) {
                    String newValueOf2gram = "";
                    if(value.toString().split("\t").length != 2) {
                        newValueOf2gram = original2GramValue + value.toString(); //Now the 3gram is in the 5th value 
                        context.write(new Text(words[1]) , new Text(newValueOf2gram));
                    }    
                }    
            }        
        }            
    }

    public static class PartitionerClass2 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2 - First Join");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass2.class);
        job.setPartitionerClass(PartitionerClass2.class);
        job.setCombinerClass(ReducerClass2.class);
        job.setReducerClass(ReducerClass2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://hashem-itbarach/output/step1"));
        TextOutputFormat.setOutputPath(job, new Path("s3://hashem-itbarach/output/step2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}