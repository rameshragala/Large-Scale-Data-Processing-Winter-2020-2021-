import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NlineEmp extends Configured implements Tool{
   
    public static class NLineMapper extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
            String a = value.toString();
            String[] b = a.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
           
            if (!(b[0].equals("Name")))
                context.write(new Text(b[0]), new Text(b[2]));
        }
    }

    public static class NLineReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            for (Text values : value){
                context.write(key, values);
            }
        }
    }
   
/*    public static class NLineEmpInputFormat extends FileInputFormat<LongWritable,Text>{
        public static final String  LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";
       
        public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, TaskAttemptContext context) throws IOException{
            context.setStatus(split);
            return new LineRecordReader();
        }
    } */
   
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 2); // extra line for Nlineinput format
       
        //Job job = new Job(conf,"NLine Input Format");
        Job job = Job.getInstance(conf, "NLine Input Format");
        job.setJarByClass(NlineEmp.class);

        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new NlineEmp(), args);
        System.exit(exitcode);
    }
}