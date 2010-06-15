/**
 * WordCounter - hoge fuga
 */
package net.broomie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.broomie.mapper.TokenizerMapper;
import net.broomie.reducer.TokenizerReducer;

/**
 *
 * @author kimura
 *
 */
public final class WordCounter extends Configured implements Tool {

    public final int run(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        String in = args[0];
        String out = args[1];
        runWordCount(conf, in, out);
        return 0;
    }

    private boolean runWordCount(Configuration conf, String in, String out)
        throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf);
        job.setJarByClass(WordCounter.class);
        TextInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TokenizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);

        return job.waitForCompletion(true);
    }

    public static void main(final String[] args) throws Exception {
        int rv = ToolRunner.run(new WordCounter(), args);
        System.exit(rv);
    }
}