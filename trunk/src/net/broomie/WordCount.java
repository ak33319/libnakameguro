/**
 * WordCount - the class for countint words from japanese doc.
 */
package net.broomie;

import static net.broomie.ConstantsClass.*;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import net.broomie.utils.Tokenizer;
import net.broomie.mapper.TokenizeMapper;
import net.broomie.reducer.TokenizeReducer;

/**
 *
 * @author kimura
 *
 */
public final class WordCount {

    /** private constructor. */
    private WordCount() { }

    /**
     *
     * @param args arguments from command line.
     * @throws Exception exception.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizeMapper.class);
        job.setCombinerClass(TokenizeReducer.class);
        job.setReducerClass(TokenizeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }
}
