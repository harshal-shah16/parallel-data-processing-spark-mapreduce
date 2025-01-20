package joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;


public class RepJoinApprox extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RepJoinApprox.class);
	//private enum Counter{Path2, Triangle};

	public static class SelfJoinMapper extends Mapper<Object, Text, Text, Text> {

		private HashMap<String, HashSet<String>> lookup = new HashMap<>();
		private enum Counter{Path2, Triangle};

		@Override
		public void setup(Context context) throws IOException, InterruptedException {


			Configuration conf = context.getConfiguration();
			int max = Integer.parseInt(conf.get("max"));

			try {

				//For local development
				//URI[] files = context.getCacheFiles();
				//Path edges_path = new Path(files[0]);
				//File file = new File(edges_path.toString());
				//BufferedReader rdr = new BufferedReader(new FileReader(file));

				//For Running in AWS EMR
				FileSystem fs = FileSystem.get(new Path("s3://hshah").toUri(), new Configuration());
				fs.setWorkingDirectory(new Path("/input/"));
				FSDataInputStream in = fs.open(new Path("edges.csv"));
				BufferedReader rdr = new BufferedReader(new InputStreamReader(in));


				String line;



				while ((line = rdr.readLine()) != null) {

					String[] userIds = line.split(",");
					//Filter out userIds greater than max
					//Create a hashmap from each record in edges file where the first userID in each record would be the key
					//Value would be hashset of all user ids that the key follows
					if (Integer.parseInt(userIds[0]) <= max && Integer.parseInt(userIds[1]) <= max ) {
						HashSet<String> usersFollowed = lookup.getOrDefault(userIds[0], new HashSet<String>());
						usersFollowed.add(userIds[1]);
						lookup.put(userIds[0], usersFollowed );
					}
				}

				rdr.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int max = Integer.parseInt(conf.get("max"));

			String[] userIds = value.toString().split(",");
			//Filter out userIds greater than max
			if (Integer.parseInt(userIds[0]) <= max && Integer.parseInt(userIds[1]) <= max) {

				//Get list of all userIds that the second userID in each record is following
				HashSet<String> userTwoFollowing = lookup.get(userIds[1]);

				if (userTwoFollowing != null) {
					Iterator<String> userTwoFollowingIterator = userTwoFollowing.iterator();
					//Iterate over the userIDs that userTwo
					while (userTwoFollowingIterator.hasNext()) {

						//Example (1,2) (2,3). If userTwoFollowingIterator is non-empty it would mean
						//a valid path has been found.
						context.getCounter(Counter.Path2).increment(1);
						//context.getCounter("Counter", "Path2").increment(1);

						//Example (1,2) (2,3) (3,1)
						//We need to look-up all user Ids that 3 is following
						HashSet<String> userThreeFollowing = lookup.get(userTwoFollowingIterator.next());

						if (userThreeFollowing != null) {

							Iterator<String> userThreeFollowingIterator = userThreeFollowing.iterator();
							//Example (1,2) (2,3) (3,1)
							//We iterate over userIDs that 3 follows and if we find '1' we found a triangle
							//and increment the counter
							while (userThreeFollowingIterator.hasNext()) {
								if (userThreeFollowingIterator.next().equals(userIds[0])) {
									context.getCounter(Counter.Triangle).increment(1);
									//context.getCounter("Counter", "Triangle").increment(1);
								}
							}
						}
					}
				}
			}
			//String triangleCounter = String.valueOf(context.getCounter(RepJoinApprox.Counter.Triangle));
			//String pathCounter = String.valueOf(context.getCounter(RepJoinApprox.Counter.Path2));
			//context.write(new Text(pathCounter + ""), null);
			//context.write(new Text(triangleCounter + ""), null);
		}
	}




	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("max", args[2]);
		final Job job = Job.getInstance(conf, "Rep-Join Approx Triangle Cardinality");
		job.setJarByClass(RepJoinApprox.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		//final FileSystem fileSystem = FileSystem.get(conf);
		//if (fileSystem.exists(new Path(args[1]))) {
		//	fileSystem.delete(new Path(args[1]), true);
		//}
		// ================
		job.addCacheFile(new Path(args[0] + "/edges.csv").toUri());



		job.setMapperClass(SelfJoinMapper.class);

		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.waitForCompletion(true);

		//Counters counters = job.getCounters();
		//Counter path2 = counters.findCounter(Counter.Path2);
		//logger.info(path2.getDisplayName() +":"+ path2.getValue());
				//(path2.getDisplayName() +":"+path2.getValue());
		//org.apache.hadoop.mapreduce.Counter triangle = counters.findCounter(Counter.Triangle);
		//logger.info(triangle.getDisplayName() +":"+ triangle.getValue());
		//System.out.println(triangle.getDisplayName() +":"+triangle.getValue());

		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <max>");
		}

		try {
			ToolRunner.run(new RepJoinApprox(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}