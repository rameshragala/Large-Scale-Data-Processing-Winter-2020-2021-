import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.StringTokenizer;
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws 
	IOException, InterruptedException
	{
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		while(itr.hasMoreTokens())
		{
			word.set(itr.nextToken());
			context.write(word,one);
		}
	}

}
