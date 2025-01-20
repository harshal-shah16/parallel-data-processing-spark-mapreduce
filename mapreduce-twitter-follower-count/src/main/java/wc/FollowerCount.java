package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class FollowerCount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FollowerCount.class);

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);
		private final IntWritable user = new IntWritable();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString());

			//User id of the follower from edges.csv or user id from nodes.csv
			String userIdOne = itr.nextToken(",");
			int idOne = Integer.parseInt(userIdOne);
			//If the line has only a single token, it means the id came from nodes.csv
			//Emit zero followers, if the user has follower it would come from edges.csv
			if (!itr.hasMoreTokens()) {
				if (idOne % 100 == 0) {
					user.set(idOne);
					context.write(user, zero);
				}
			}
			//If line contains two tokens, line is being read from edges.csv
			else {
				//UserId of whom followers need to be counted
				String userIdTwo = itr.nextToken();
				int idTwo = Integer.parseInt(userIdTwo);

				//Emit count of only those users whose id is divisible by 100
				boolean check = (idTwo % 100) == 0;
				if (check) {
					user.set(idTwo);
					context.write(user, one);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Follower Count");
		job.setJarByClass(FollowerCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new FollowerCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}