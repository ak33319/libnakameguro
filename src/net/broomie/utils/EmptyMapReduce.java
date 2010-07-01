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

/**
 * @author kimura
 *
 */
public final class EmptyMapReduce {

    /**
     * constructor.
     */
    private EmptyMapReduce() { }

    /**
     * @author kimura
     */
    public static class EmptyMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        /** one. */
        private static final IntWritable ONE = new IntWritable(1);

        /** word.*/
        private Text word = new Text();

        @Override
        public final void map(Object key, Text value, Context context) {
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

    /**
     * @author kimura
     */
    public static class EmptyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key hoge.
         * @param values hoge.
         * @param context hoge.
         */
        public final void reduce(Text key, Iterable<IntWritable> values,
                Context context) {
            for (IntWritable val : values) {
                System.err.println(val);
            }
            //context.write(key, value);
        }
    }

    /**
     * @param args arguments.
     * @throws Exception exception.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: EmptyMapReduce <in> <out>");
            System.exit(-1);
        }
        Job job = new Job(conf, "EmptyMapReduce");
        job.setJarByClass(EmptyMapReduce.class);
        job.setMapperClass(EmptyMapper.class);
        job.setReducerClass(EmptyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 1 : 1);

    }
}
