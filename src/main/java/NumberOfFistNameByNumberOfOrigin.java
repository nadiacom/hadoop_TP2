import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class NumberOfFistNameByNumberOfOrigin {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //For each line passed to the mapper, split on ";" and remove spaces
            String[] line = value.toString().replaceAll(", ",",").split(";");
            //Get origin (2nd value), the line may contain numerous origins separated by ","
            String[] originfromLine = line[2].split(",");
            //Count number of origin
            int nbOrigin = 0;
            for(String origin : originfromLine){
                //If origin is not empty
                if(!origin.equals("")) {
                    //Increment counter
                    nbOrigin++;
                }
            }
            //Output (nb of origin counter,1)
            word.set(Integer.toString(nbOrigin));
            context.write(word,one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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
        Job job = Job.getInstance(conf, "Count number of first name by number of origin");
        job.setJarByClass(NumberOfFistNameByNumberOfOrigin.class);
        job.setMapperClass(NumberOfFistNameByNumberOfOrigin.TokenizerMapper.class);
        job.setCombinerClass(NumberOfFistNameByNumberOfOrigin.IntSumReducer.class);
        job.setReducerClass(NumberOfFistNameByNumberOfOrigin.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
