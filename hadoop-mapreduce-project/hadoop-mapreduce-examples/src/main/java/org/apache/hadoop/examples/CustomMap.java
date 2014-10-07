/**
 * 
 */
package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author oxhead
 * 
 */
public class CustomMap {

	public static final String CONF_TIME_PERIOD = "myconf.time_period";
	public static final String CONF_NUM_CPU_WORKERS = "myconf.num_cpu_workers";
	public static final String CONF_NUM_VM_WORKERS = "myconf.num_vm_workers";
	public static final String CONF_NUM_VM_BYTES = "myconf.num_vm_bytes";

	public static class WordMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

		int timeout = 1;
		int cpu_workers = 1;
		int vm_workers = 1;
		int vm_bytes = 1024000;

		int readCount = 0;
		int readBytes = 0;
		int targetCount = 1000;
		int targetBytes = 1024 * 1024 * 4;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (timeout > 0) {
				readBytes += value.getLength();
				readCount++;
				if (readBytes >= targetBytes) {
					perform();
					readBytes -= targetBytes;
				} else if (readCount >= targetCount) {
					perform();
					readCount -= targetCount;
				}
			}
		}

		protected void perform() {
			try {
				String[] commands = new String[] { "/usr/bin/stress", "--timeout", String.valueOf(timeout) + "s", "--cpu", String.valueOf(cpu_workers), "--vm", String.valueOf(vm_workers),
						"--vm-bytes", String.valueOf(vm_bytes) + "B" };

				Process child = Runtime.getRuntime().exec(commands);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			timeout = context.getConfiguration().getInt(CONF_TIME_PERIOD, 1);
			cpu_workers = context.getConfiguration().getInt(CONF_NUM_CPU_WORKERS, 1);
			vm_workers = context.getConfiguration().getInt(CONF_NUM_VM_WORKERS, 1);
			vm_bytes = context.getConfiguration().getInt(CONF_NUM_VM_BYTES, 1024000);
			readCount = 0;
			readBytes = 0;

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err.println("Usage: custommap <in> <out> <time_period> <cpu_workers> <vm_workers> <vm_bytes>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, CustomMap.class.getCanonicalName());
		job.getConfiguration().set(CONF_TIME_PERIOD, otherArgs[2]);
		job.getConfiguration().set(CONF_NUM_CPU_WORKERS, otherArgs[3]);
		job.getConfiguration().set(CONF_NUM_VM_WORKERS, otherArgs[4]);
		job.getConfiguration().set(CONF_NUM_VM_BYTES, otherArgs[5]);
		job.setJobName(CustomMap.class.getCanonicalName() + "_" + otherArgs[2] + "_" + otherArgs[3] + "_" + otherArgs[4] + "_" + otherArgs[5]);

		job.setJarByClass(CustomMap.class);
		job.setMapperClass(WordMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
