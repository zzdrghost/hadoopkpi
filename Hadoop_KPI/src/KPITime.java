import java.io.IOException;

import java.text.ParseException;
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


public class KPITime {

    public static class KPITimeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context output) 
        		throws IOException,InterruptedException {
            KPI kpi = KPI.filterTime(value.toString());
            if (kpi.isValid()) {
                try {
                    word.set(kpi.getTime_local_Date_hour());
                    output.write(word, one);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class KPITimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context output) 
        		throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            output.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "master:9001");
        conf.set("mapred.jar", "Hadoop_KPI.jar");       
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		    System.err.println("Usage: KPITime <in> <out>");
		    System.exit(2);
		}			
		Job job = new Job(conf, "KPITime");
		job.setJarByClass(KPITime.class);
		job.setMapperClass(KPITimeMapper.class);
        job.setCombinerClass(KPITimeReducer.class);
        job.setReducerClass(KPITimeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
