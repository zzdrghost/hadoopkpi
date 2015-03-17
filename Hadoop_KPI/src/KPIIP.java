import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KPIIP {

    public static class KPIIPMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text ips = new Text();
        
        public void map(Object key, Text value, Context output) 
        		throws IOException, InterruptedException {
            KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                output.write(word, ips);
            }
        }
    }

    public static class KPIIPReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> ipset = new HashSet<String>();
        //private IntWritable count = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context output) 
        		throws IOException, InterruptedException {
        	for (Text val : values) {
                ipset.add(val.toString());
            }
            result.set(String.valueOf(ipset.size()));
            output.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "master:9001");
        conf.set("mapred.jar", "Hadoop_KPI.jar");       
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		    System.err.println("Usage: KPIIP <in> <out>");
		    System.exit(2);
		}			
		Job job = new Job(conf, "KPIIP");
		job.setJarByClass(KPIIP.class);
		job.setMapperClass(KPIIPMapper.class);
        job.setCombinerClass(KPIIPReducer.class);
        job.setReducerClass(KPIIPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setMapOutputKeyClass(Text.class);        
        //job.setMapOutputValueClass(Text.class);        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
