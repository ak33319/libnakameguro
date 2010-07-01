package net.broomie.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public final class grepUtil {

    private grepUtil() { }

    public static class grepUtilMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        private Text word = new Text();

            @Override
            public final void map(Object key, Text value, Context context) {

                Configuration conf = context.getConfiguration();
                String target = conf.get("targetString");
                System.err.println("[CHECK]" + target);
                String buf = value.toString();
                if (buf.indexOf(target) != -1) {
                    word.set(target);
                    try {
                        context.write(word, ONE);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }
    }

    public static class grepUtilReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    }

    /**
     * hoge.
     * @param args hoge
     * @throws IOExceptione fuga.
     * @throws InterruptedException hoge
     * @throws ClassNotFoundException hoge
     */
    public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        conf.set("targetString", args[2]);
        Job job = new Job(conf, "grepUtil");
        job.setJarByClass(grepUtil.class);
        job.setMapperClass(grepUtilMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
