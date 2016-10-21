import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaleFemaleProportion {

    public enum COUNTER_TOTAL{
        GENDER
    };

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //For each line passed to the mapper , split on ";" and remove spaces
            String[] line = value.toString().replaceAll(", ",",").split(";");
            //Get gender (1st value), the line may contain numerous gender (male and female) separated by ","
            String[] genders = line[1].split(",");
            //If (f,m) -> output "both"
            if (genders.length>1){
                word.set("Both");
                context.write(word, one);
            }//Else if (f) -> output "female"
            else if(genders[0].contains("f")){
                word.set("Female");
                context.write(word, one);
            }
            //Else if (m) -> output male
            else if(genders[0].contains("m")){
                word.set("Male");
                context.write(word, one);
            }
            //For each line, increment the global counter GENDER
            context.getCounter(COUNTER_TOTAL.GENDER).increment(1);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Get the global counter GENDER
            long total_lines = context.getCounter(COUNTER_TOTAL.GENDER).getValue();
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //Output percentage of male, female, both
            result.set((int)((long)sum*100/total_lines));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Proportion of male or female");
        job.setJarByClass(MaleFemaleProportion.class);
        job.setMapperClass(MaleFemaleProportion.TokenizerMapper.class);
        job.setCombinerClass(MaleFemaleProportion.IntSumReducer.class);
        job.setReducerClass(MaleFemaleProportion.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
