package td;

import java.io.BufferedReader;
//import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class code extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(code.class);
	static int length = 0;
	static Path vocab_file;
	static ArrayList<String> vocabList = new ArrayList<String>();

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		vocab_file = new Path(args[0]);
		Path input = new Path(args[1]);
		Path output = new Path(args[2]);

		FileSystem fs = FileSystem.get(conf);
		ContentSummary cs = fs.getContentSummary(input);
		length = (int) cs.getFileCount();
		// System.out.println("************ " +length);

		// length = new File(args[0]).list().length;
		// System.out.println(length);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(vocab_file)));
		String word = br.readLine();
		while (word != null) {
			vocabList.add(word.toLowerCase().trim());
			word = br.readLine();
		}

		Job job1 = Job.getInstance(conf, "wordInDoc");
		job1.setJarByClass(code.class);

		job1.setMapperClass(Map1.class);
		job1.setCombinerClass(Reduce1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, new Path(output, "output1"));

		if (!(job1.waitForCompletion(true)))
			System.exit(1);

		Job job2 = Job.getInstance(conf, "finalTFIDF");
		job2.setJarByClass(code.class);

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(output, "output1"));
		FileOutputFormat.setOutputPath(job2, new Path(output, "output2"));

		if (!(job2.waitForCompletion(true)))
			System.exit(1);

	}

	public int run(String[] args) throws Exception {

		return 1;
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
			String[] parsePath = filePathString.split("/");
			filePathString = parsePath[parsePath.length - 1];
			String line = value.toString();
			line = line.toLowerCase();

			Pattern p = Pattern.compile("([\\w-]+)", Pattern.CASE_INSENSITIVE);
			// Matcher m = p.matcher(line);
			String words[] = line.split(" ");

			for (String s : words) {
				Matcher m = p.matcher(s);
				if (!vocabList.contains(s.toLowerCase()) && m.find())
					s = m.group();
				if (vocabList.contains(s.toLowerCase())) {
					s = s + "@" + filePathString;
					Text addThis = new Text(s);
					context.write(addThis, one);
				}
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counter, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counter) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String string = value.toString();
			String[] parseValues = string.split("\t");
			String[] parseNames = parseValues[0].split("@");

			Text word = new Text(parseNames[0]);
			Text docAndCount = new Text(parseNames[1] + "=" + parseValues[1]);
			// System.out.println(word+", "+ docAndCount);
			context.write(word, docAndCount);

		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> wordCount, Context context)
				throws IOException, InterruptedException {

			String wordString = word.toString();
			List<String> cache = new ArrayList<String>();
			int totalDocs = 0;
			for (Text count : wordCount) {
				String string = count.toString();
				cache.add(string);
				totalDocs++;
			}

			for (String string : cache) {
				String[] parseValues = string.split("=");
				// System.out.println(string);
				String docName = parseValues[0];
				double wordcount = Double.valueOf(parseValues[1]);
				double tf = wordcount;
				double preidf = ((double) length) / ((double) totalDocs);
				double idf = Math.log(preidf) / Math.log(2);
				double tfidf = tf * idf;

				// System.out.println(wordString+"\t"+docName +"\t"+ tfidf);
				Text wordAndDoc = new Text(wordString + "\t" + docName);
				Text wordAndCount = new Text(String.valueOf(tfidf));
				context.write(wordAndDoc, wordAndCount);
			}
		}
	}
}
