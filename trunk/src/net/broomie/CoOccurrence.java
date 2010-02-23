/**
 * CoOccurrence co-occurrence word count class.
 */
package net.broomie;

import java.io.IOException;

import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static net.broomie.ConstantsClass.*;
import java.util.EnumSet;
import java.util.Set;
import java.util.Iterator;

import net.broomie.MyPriorityQueue;


/**
 *
 * @author kimura
 *
 */
public final class CoOccurrence {

    /** private constructor. */
    private CoOccurrence() { }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccurrenceMapper extends
        Mapper<Object, Text, Text, DoubleWritable> {

        /** key object. */
        private Text mapKey = new Text();

        /** value object. */
        private DoubleWritable mapVal = new DoubleWritable();

        /** Tokenizer Instance. */
        private Tokenizer tokenizer = new Tokenizer();

        /**
         *
         * @param key key object.
         * @param value value object.
         * @param context context object.
         * @throws IOException exception for IO.
         * @throws InterruptedException exception.
         */
        @Override
        public final void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            String buf = value.toString();
            if (buf.length() > MAX_LINE_LENGTH) {
                buf = buf.substring(0, MAX_LINE_LENGTH);
            }
            String[] result = tokenizer.getToken(buf, EnumSet
                    .of(Tokenizer.ExtractType.Noun));

            int resultLength = result.length;

            for (int i = 0; i < resultLength; i++) {

                for (int j = 1; j < 5; j++) {
                    if (i - j >= 0) {
                        mapVal.set(1.2);
                        mapKey.set(result[i] + "\t" + result[i - j]);
                        context.write(mapKey, mapVal);
                    }
                    if (i + j < resultLength) {
                        mapVal.set(1 * 1.2);
                        mapKey.set(result[i] + "\t" + result[i + j]);
                        context.write(mapKey, mapVal);
                    }
                }
            }
        }
    }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccurrenceReducer extends
        Reducer<Text, DoubleWritable, Text, Text> {

        /**
         * aaa.
         */
        private Text resultVal = new Text();

        /**
         * aaaa.
         */
        private MyPriorityQueue queue = new MyPriorityQueue(100);


        @Override
        public final void reduce(Text key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {

            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            resultVal.set(Double.toString(sum));
            context.write(key, resultVal);
        }
    }

    /**
     *
     * @param args command line options.
     * @throws Exception execpt.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "co-occurrence");
        job.setJarByClass(CoOccurrence.class);
        job.setMapperClass(CoOccurrenceMapper.class);
        job.setReducerClass(CoOccurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(2);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }
}
