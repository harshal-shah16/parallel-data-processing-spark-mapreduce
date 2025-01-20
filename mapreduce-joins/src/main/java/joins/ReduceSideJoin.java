package joins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class ReduceSideJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ReduceSideJoin.class);
	private enum Counter{Path2};



	public static class SelfJoinMapper extends Mapper<Object, Text, Text, Text> {
		private final Text keyOne = new Text();
		private final Text valueOne = new Text();
		private final Text keyTwo = new Text();
		private final Text valueTwo = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString());
			String userIdOne = itr.nextToken(",");
			String userIdTwo = itr.nextToken();
			
			keyOne.set(userIdOne);
			valueOne.set("A" + userIdTwo);
			keyTwo.set(userIdTwo);
			valueTwo.set("B" + userIdOne);
			context.write(keyOne, valueOne);
			context.write(keyTwo, valueTwo);
		}
	}


	public static class SelfJoinReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int m = 0;
			int n = 0;


			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					m++;
				} else if (t.charAt(0) == 'B') {
					n++;
				}
			}
			//m*n would represent the number of Path2 with key as the middle node
			context.getCounter(Counter.Path2).increment(m * n);
		}
	}






	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "ReduceSide Join");
		job.setJarByClass(ReduceSideJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		// ================
		job.setMapperClass(SelfJoinMapper.class);
		job.setReducerClass(SelfJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ReduceSideJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}