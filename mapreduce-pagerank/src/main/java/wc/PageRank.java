package wc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRank.class);
	private enum Counter{dummyPR};

	public static class PageRankMapper extends Mapper<Object, Text, IntWritable, Node> {
		int k = 3;
		double totalDanglingPRMass;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			//Capture PR of dummy node zero and distribute it to all nodes
			totalDanglingPRMass = Double.parseDouble(conf.get("totalDanglingPRMass"));

		}

		@Override
		public void map(final Object key, final Text inputNode, final Context context) throws IOException, InterruptedException {

			//Convert the record into a node
			Node node = new Node(inputNode);

			//Distribute page rank of dummy node to all other nodes
			double newPageRank =  node.getPageRank() + 0.85 * (totalDanglingPRMass * (1.0 / (k * k)));


			//Emit this node - Pass along the graph structure to reducers
			//Node emitNode = new Node(node.getSelf(), node.getToPage(), newPageRank);
			node.setPageRank(newPageRank);
			context.write(new IntWritable(node.getSelf()), node);


			int toPage = node.getToPage();

			//Send node's contributions to outlinks. Since there eahc node has only 1 outlink
			//We send the entire node's contribution
			//Setting node's value to -1 indicates that this node data object is an incoming page rank contribution towards the node indicated by key
			context.write(new IntWritable(toPage), new Node(-1, -1, newPageRank));

		}
	}

	public static class PageRankReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
		int k = 3;

		@Override
		public void reduce(final IntWritable key, final Iterable<Node> values, final Context context) throws IOException, InterruptedException {

				Node node = new Node();
				double totalIncomingContributions = 0.0;

				for(Node itr : values) {

					if (itr.getSelf() == -1) {
						//contribution is stored in pagerank property
						totalIncomingContributions += itr.getPageRank();
					} else {
						//Recover node
						//node = new Node(itr.getSelf(), itr.getToPage(), itr.getPageRank());
						node.setSelf(itr.getSelf());
						node.setToPage(itr.getToPage());
					}
				}

				double newPR = 0.0;
				if (node.getSelf() == 0) {
					//Set the new page rank of dummy node to 0
					newPR = 0.0;
				} else {
					//Calculate the new Page Rank of the node
					newPR = 0.15 * (1.0/k*k) + 0.85 * (totalIncomingContributions);
				}

				//Set the new Page rank to node
				node.setPageRank(newPR);

				//Update global PR counter of dummy node zero. Has to be converted to long or int because counters do not store double
				if (node.getSelf() == 0) {
					long dummyPRvalue = (long) (totalIncomingContributions * 1000000000);
					context.getCounter(PageRank.Counter.dummyPR).increment(dummyPRvalue);
				}

				context.write(new IntWritable(node.getSelf()), node);

		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		//First Job/Iterataion
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "PageRank");
		job.setJarByClass(PageRank.class);
		Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", " ");
		job.getConfiguration().set("totalDanglingPRMass", ""+ 0.0);

		job.setMapperClass(PageRankMapper.class);
		job.setCombinerClass(PageRankReducer.class);
		job.setReducerClass(PageRankReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Node.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1], "1"));


		if(!job.waitForCompletion(true)) return -1;


		int iterations = 2;

		//Set the output of first iteration as input to the next iteration
		Path inputPath = new Path(args[1], iterations - 1 + "" );
		Path outputPath = new Path(args[1], iterations + "");



		while (iterations <= 10) {

			long dummyPRPreviousJobLong = job.getCounters().findCounter(Counter.dummyPR).getValue();
			double dummyPRPreviousJobDouble = (double) dummyPRPreviousJobLong / 1000000000.0;
			job = Job.getInstance(conf, "PageRank" + iterations);
			job.setJarByClass(PageRank.class);
			jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", " ");
			job.getConfiguration().set("totalDanglingPRMass", "" + dummyPRPreviousJobDouble);

			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			job.setMapperClass(PageRankMapper.class);
			job.setCombinerClass(PageRankReducer.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Node.class);

			if(!job.waitForCompletion(true)) return -1;

			iterations++;

			inputPath = new Path(args[1], iterations - 1 + "");
			outputPath = new Path(args[1], iterations + "");


		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new PageRank(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}


//Node class to create node objects encapsulating self page value, connected to page value and self PR value
class Node implements Writable {

	private int self;
	private int toPage;
	private double pageRank;

	Node() {}


	Node(Text inputNode) {

		String inputNodeToString = inputNode.toString();
		setSelf(inputNodeToString);
		setToPage(inputNodeToString);
		setPageRank(inputNodeToString);

	}

	Node (int self, int toPage, double pageRank) {
		this.self = self;
		this.toPage = toPage;
		this.pageRank =  pageRank;

	}

	private void setPageRank(String inputNodeToString) {
		this.pageRank = Double.parseDouble(inputNodeToString.split(" ")[2]);
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	private void setToPage(String inputNodeToString) {
		this.toPage = Integer.parseInt(inputNodeToString.split(" ")[1]);
	}

	private void setSelf(String inputNodeToString) {
		this.self = Integer.parseInt(inputNodeToString.split(" ")[0]);
	}
	public void setSelf(int self) {
		this.self = self;
	}

	public void setToPage(int toPage) {
		this.toPage = toPage;
	}

	public int getSelf() {
		return this.self;
	}

	public int getToPage() {
		return this.toPage;
	}

	public double getPageRank() {
		return this.pageRank;
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(self);
		dataOutput.writeInt(toPage);
		dataOutput.writeDouble(pageRank);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {

		this.self = dataInput.readInt();
		this.toPage = dataInput.readInt();
		this.pageRank = dataInput.readDouble();
	}

	@Override
	public String toString() {
		return "" + this.toPage + " " + String.format("%1.10f",this.pageRank);
	}
}