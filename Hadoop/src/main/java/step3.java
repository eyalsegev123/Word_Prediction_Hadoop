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
            String[] words = key.split(" "); // Split by tab

            if(words.length != 1){
                System.out.println("word length should be 1");                
                return;
            }
            else
                context.write(textKey, textValue);
        }
        
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            String[] words = key.toString().split(" "); // Split by tab

            if(words.length != 1){
                System.out.println("word length should be 1");                
                return;
            }
            
            
            
             //Length is 1 as it should
            String original1GramValue = null;
            for(Text value: values) {
                if(value.toString().split("\t").length == 1) {
                    original1GramValue = value.toString();
                    break; 
                }  
            }   
                               
            for (Text value : values) {
                String newValueOf2gram = "";
                if(value.toString().split("\t").length != 1) {
                    newValueOf1gram = original2GramValue + value.toString(); //Now the 3gram is in the 6th value
                    String threeGramWithNumber = value.toString().split("\t")[4];
                    int indexOfColon = threeGramWithNumber.lastIndexOf(":");
                    String threeGram = threeGramWithNumber.substring(0, indexOfColon);
                    context.write(new Text(threeGram) , new Text(newValueOf1gram));
                }    
            }    
        }        
    }            

    public static class PartitionerClass extends Partitioner<Text, IntWritable> { //Get partition wasnt done today
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }
}