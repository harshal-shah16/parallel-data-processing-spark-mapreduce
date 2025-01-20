package joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class ReduceSideJoinTriangle extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ReduceSideJoinTriangle.class);
	private enum Counter{Path2, Triangle};




	public static class SelfJoinMapper extends Mapper<Object, Text, Text, Text> {
		private final Text keyOne = new Text();
		private final Text valueOne = new Text();
		private final Text keyTwo = new Text();
		private final Text valueTwo = new Text();


		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int max = Integer.parseInt(conf.get("max"));

			final StringTokenizer itr = new StringTokenizer(value.toString());
			String userIdOne = itr.nextToken(",");
			String userIdTwo = itr.nextToken();

			if (Integer.parseInt(userIdOne) <= max && Integer.parseInt(userIdTwo) <= max) {
				keyOne.set(userIdOne);
				valueOne.set("A" + userIdTwo);
				keyTwo.set(userIdTwo);
				valueTwo.set("B" + userIdOne);
				context.write(keyOne, valueOne);
				context.write(keyTwo, valueTwo);
			}
		}
	}


	public static class SelfJoinReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();


		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			listA.clear();
			listB.clear();


			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					String toAdd = t.toString().substring(1);
					listA.add( new Text(key + "," + toAdd));
				} else if (t.charAt(0) == 'B') {
					String toAdd = t.toString().substring(1);
					listB.add(new Text(toAdd));
				}
			}

			executeJoinLogic(context);
		}

		public void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if (!listA.isEmpty() && !listA.isEmpty()) {
				for (Text A : listA) {
					for (Text B : listB) {
						context.write(B, A);
						context.getCounter(Counter.Path2).increment(1);
					}
				}
			}
		}
	}


	public static class MapperTwo extends Mapper<Object, Text, Text, Text> {
		private final Text keyOne = new Text();
		private final Text valueOne = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			int max = Integer.parseInt(conf.get("max"));

			String[] tokens = value.toString().split(",");

			if (tokens.length == 3) {
				keyOne.set(tokens[2]);
				valueOne.set("A" + tokens[0] + "," + tokens[1]);
				context.write(keyOne, valueOne);
			}

			else if (tokens.length == 2){
				//Filter out userIds greater than max
				if (Integer.parseInt(tokens[0]) <= max && Integer.parseInt(tokens[1]) <= max) {
					keyOne.set(tokens[0]);
					valueOne.set("B" + tokens[1]);
					context.write(keyOne, valueOne);
				}
			}
		}
	}


	public static class UserJoinReducerTwo extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();


		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			listA.clear();
			listB.clear();


			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					String toAdd = t.toString().substring(1);
					listA.add(new Text(toAdd));
				} else if (t.charAt(0) == 'B') {
					String toAdd = t.toString().substring(1);
					listB.add(new Text(toAdd));
				}
			}

			executeJoinLogic(context);

		}

		public void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if (!listA.isEmpty() && !listA.isEmpty()) {
				for (Text A : listA) {
					String[] splitA = A.toString().split(",");

					for (Text B : listB) {

						//String[] splitB = B.toString().split(",");
						if (splitA[0].equals(B.toString())) {
							context.write(A, B);
							context.getCounter(Counter.Triangle).increment(1);
						}
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("max", args[2]);
		final Job job = Job.getInstance(conf, "RSJoin Triangle Cardinality MR 1");
		job.setJarByClass(ReduceSideJoinTriangle.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		//final FileSystem fileSystem = FileSystem.get(conf);
		//if (fileSystem.exists(new Path(args[1]))) {
		//	fileSystem.delete(new Path(args[1]), true);
		//}
		// ================
		job.setMapperClass(SelfJoinMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(SelfJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.waitForCompletion(true);

		//Second MR Job configuration
		final Configuration conf2 = getConf();
		final Job job2 = Job.getInstance(conf2, "RSJoin Triangle Cardinality MR 2");
		job2.setJarByClass(ReduceSideJoinTriangle.class);

		conf2.set("mapreduce.output.textoutputformat.separator", ",");
		//if (fileSystem.exists(new Path("RSJoinTriangle"))) {
		//	fileSystem.delete(new Path("RSJoinTriangle"), true);
		//}
		job2.setMapperClass(MapperTwo.class);
		job2.setReducerClass(UserJoinReducerTwo.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, MapperTwo.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, MapperTwo.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <output-dir> <max> <output2-dir>");
		}

		try {
			ToolRunner.run(new ReduceSideJoinTriangle(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}