import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TotalSalesWithDC extends Configured implements Tool{
    // Mapper
    public static class TotalSalesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text item = new Text();
        public void map(LongWritable key, Text value, Context context) 
                 throws IOException, InterruptedException {
             // Splitting the line on tab
             String[] stringArr = value.toString().split("\t");
             item.set(stringArr[0] + " " + stringArr[2]);             
             Integer sales = Integer.parseInt(stringArr[1]);
             context.write(item, new IntWritable(sales));
         }
    }
    // Reducer
    public static class TotalSalesReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private Properties cityProp = new Properties();
        private Text cityKey = new Text();
        private IntWritable result = new IntWritable();
        @Override 
        protected void setup(Context context) throws IOException, InterruptedException { 
            // That's where file stored in distributed cache is used 
            InputStream iStream = new FileInputStream("./city");  
            //Loading properties
            cityProp.load(iStream);
        }
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;        
            String[] stringArr = key.toString().split(" ");
            // Getting the city name from prop file
            String city = cityProp.getProperty(stringArr[1].trim());
            cityKey.set(stringArr[0] + "\t"+ city);
            for (IntWritable val : values) {
                sum += val.get();
            }   
            result.set(sum);
            context.write(cityKey, result);
        }
    }
    public static void main(String[] args) throws Exception {
        int exitFlag = ToolRunner.run(new TotalSalesWithDC(), args);
        System.exit(exitFlag);
    }
    
    @Override
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TotalSales");
        job.setJarByClass(getClass());
        job.setMapperClass(TotalSalesMapper.class); 
        job.setReducerClass(TotalSalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Adding file to distributed cache
        job.addCacheFile(new URI("/test/input/City.properties#city"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
