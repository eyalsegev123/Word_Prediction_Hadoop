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

public class step2 {
    
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {
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
            else //check with malawach/hava if we got 2 we need to send also the second word
                context.write(textKey, textValue);
        }
        
    }

    // yeled : 40 - send as it is
    // yeled tov : 50 - search yeled and send yeled tov yeled
    // yeled nehmad :90
    // yeled tov halah : 60
    // yeled nehmad halah : 70

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            if(key.toString().split(" ").length == 1) {
                String valueOf1gram = "";
                for(Text value : values)  // Is this for relevant or not???
                    valueOf1gram += value.toString();
                context.write(key, new Text(valueOf1gram));
            }

            if(word.length != 2){
                system.out.println("error: got a 3gram");
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
                String [] words = key.toString().split(" ");
                
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

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }
}