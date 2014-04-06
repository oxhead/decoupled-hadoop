package org.apache.hadoop.examples.puma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * This is an example Hadoop Map/Reduce application. It reads the input movie
 * files and outputs a histogram showing how many reviews fall in which range We
 * make 5 reduce tasks for ratings of 1, 2, 3, 4, and 5. the reduce task counts
 * all reviews in the same bin and emits them.
 * 
 * To run: bin/hadoop jar build/hadoop-examples.jar histogram_ratings [-m
 * <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
 * 
 * @author Faraz Ahmad
 */
@SuppressWarnings("deprecation")
public class HistogramRatings extends Configured implements Tool {

	private enum Counter {
		WORDS, VALUES
	}

	public static final Log LOG = LogFactory.getLog(HistogramRatings.class);

	public static class MapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int rating, reviewIndex, movieIndex;
			String reviews = new String();
			String tok = new String();
			String ratingStr = new String();

			String line = ((Text) value).toString();
			movieIndex = line.indexOf(":");
			if (movieIndex > 0) {
				reviews = line.substring(movieIndex + 1);
				StringTokenizer token = new StringTokenizer(reviews, ",");
				while (token.hasMoreTokens()) {
					tok = token.nextToken();
					reviewIndex = tok.indexOf("_");
					ratingStr = tok.substring(reviewIndex + 1);
					rating = Integer.parseInt(ratingStr);
					context.write(new IntWritable(rating), one);
					context.getCounter(Counter.WORDS).increment(1);
				}
			}
		}

	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += ((IntWritable) iterator.next()).get();
				context.getCounter(Counter.VALUES).increment(1);
			}
			context.write(key, new IntWritable(sum));
		}

	}

	static void printUsage() {
		System.out.println("histogram_ratings [-m <maps>] [-r <reduces>] <input> <output>");
		System.exit(1);
	}

	/**
	 * The main driver for histogram_ratings map/reduce program. Invoke this
	 * method to submit the map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("histogram-ratings");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		conf.set("mapreduce.job.maps", "8");
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.set("mapreduce.job.maps", args[++i]);
				} else if ("-r".equals(args[i])) {
					conf.set("mapreduce.job.reduces", args[++i]);
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				printUsage(); // exits
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + other_args.size() + " instead of 2.");
			printUsage();
		}

		FileInputFormat.addInputPath(job, new Path(other_args.get(0)));
		String outPath = new String(other_args.get(1));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new HistogramRatings(), args);
		System.exit(ret);
	}
}
