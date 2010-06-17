package net.broomie;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.broomie.mapper.TokenizeMapper;
import net.broomie.reducer.TokenizeReducer;
import net.broomie.mapper.CoCounteMapper;
import net.broomie.reducer.CoCounteReducer;

import static net.broomie.ConstantsClass.*;

/**
 *
 * @author kimura
 *
 */
public final class WordCoCounter extends Configured implements Tool {

    public final int run(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        conf.addResource("conf/libnakameguro.xml");
        String in = args[0];
        String out = args[1];
        String dfdb = conf.get("libnakameguro.dfdb");
        runWordCount(conf, in, dfdb);
        try {
            DistributedCache.addCacheFile(new URI(dfdb), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        runWordCoCount(conf, in, out);

        return 0;
    }

    private boolean runWordCoCount(Configuration conf, String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf);
        job.setJarByClass(WordCoCounter.class);
        TextInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setMapperClass(CoCounteMapper.class);
        job.setReducerClass(CoCounteReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        return job.waitForCompletion(true);
    }


   private boolean runWordCount(Configuration conf, String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
       Job job = new Job(conf);
       job.setJarByClass(WordCoCounter.class);
       TextInputFormat.addInputPath(job, new Path(in));
       FileOutputFormat.setOutputPath(job, new Path(out));
       job.setMapperClass(TokenizeMapper.class);
       job.setReducerClass(TokenizeReducer.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       job.setNumReduceTasks(2);

       return job.waitForCompletion(true);
   }

    /**
     * private constructor.
     */
    private WordCoCounter() {}

    public static void main(final String[] args) throws Exception {
        int rv = ToolRunner.run(new WordCoCounter(), args);
        System.exit(rv);
    }

}
