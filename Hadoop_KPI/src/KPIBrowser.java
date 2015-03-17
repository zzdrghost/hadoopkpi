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
import org.apache.hadoop.util.GenericOptionsParser;


public class KPIBrowser {

    public static class KPIBrowserMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context output) 
        		throws IOException,InterruptedException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getHttp_user_agent());
                output.write(word, one);
            }
        }
    }

    public static class KPIBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		    System.err.println("Usage: KPIBrowser <in> <out>");
		    System.exit(2);
		}			
		Job job = new Job(conf, "KPIBrowser");
		job.setJarByClass(KPIBrowser.class);
		job.setMapperClass(KPIBrowserMapper.class);
        job.setCombinerClass(KPIBrowserReducer.class);
        job.setReducerClass(KPIBrowserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
