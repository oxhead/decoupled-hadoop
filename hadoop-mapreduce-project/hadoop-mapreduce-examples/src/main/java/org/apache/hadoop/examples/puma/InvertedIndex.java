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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * This is an example Hadoop Map/Reduce application. It reads the text input
 * files, breaks each line into words Map emits <word, docid> with each word
 * emitted once per document Reduce takes map output and emits <word,
 * list(docid)>
 * 
 * To run: bin/hadoop jar build/hadoop-examples.jar invertedindex [-m
 * <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
 * 
 * @see https://sites.google.com/site/farazahmad/pumabenchmarks
 * 
 * @author Faraz Ahmad
 * @author Chin-Jung Hsu
 */

@SuppressWarnings("deprecation")
public class InvertedIndex extends Configured implements Tool {

	private enum Counter {
		WORDS, VALUES
	}

	public static final Log LOG = LogFactory.getLog(InvertedIndex.class);

	/**
	 * For each line of input, break the line into words and emit them as
	 * (<b>word,doc</b>).
	 */

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		String path;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			path = ((FileSplit) context.getInputSplit()).getPath().toString();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String docName = new String("");
			Text docId, wordText;
			String line;

			StringTokenizer tokens = new StringTokenizer(path, value.toString());
			while (tokens.hasMoreTokens()) {
				docName = tokens.nextToken();
			}
			docId = new Text(docName);
			line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				wordText = new Text(itr.nextToken());
				context.write(wordText, docId);
			}
		}
	}

	/**
	 * The reducer class
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		String str1 = new String();
		String str2 = new String();
		String valueString = new String("");
		Text docId;
		private List<String> duplicateCheck = new ArrayList<String>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			duplicateCheck = new ArrayList<String>();
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext()) {
				valueString = iterator.next().toString();
				if (duplicateCheck.contains(valueString)) {
					// skip and do not emit
				} else {
					duplicateCheck.add(valueString);
					docId = new Text(valueString);
					context.write(key, docId);
					context.getCounter(Counter.VALUES).increment(1);
				}
			}
		}

	}

	static void printUsage() {
		System.out.println("invertedindex [-m <maps>] [-r <reduces>] <input> <output>");
		System.exit(1);
	}

	/**
	 * The main driver for map/reduce program. Invoke this method to submit the
	 * map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "inverted-index");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

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
		int ret = ToolRunner.run(new InvertedIndex(), args);
		System.exit(ret);
	}

}
