/**
 * CoOccurrence co-occurrence word count class.
 */
package net.broomie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static net.broomie.ConstantsClass.*;
import java.util.EnumSet;



/**
 *
 * @author kimura
 *
 */
public final class CoOccurrenceDF {

    /** private constructor. */
    private CoOccurrenceDF() { }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccurrenceDFMapper extends
        Mapper<Object, Text, Text, IntWritable> {

        /** key object. */
        private Text mapKey = new Text();

        /** value object. */
        private IntWritable mapVal = new IntWritable(1);

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
                        mapKey.set(result[i] + "\t" + result[i - j]);
                        context.write(mapKey, mapVal);
                    }
                    if (i + j < resultLength) {
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
    public static class CoOccurrenceDFReducer extends
        Reducer<Text, IntWritable, Text, Text> {

        /**
         * aaa.
         */
        private Text resultVal = new Text();

        /**
         * aaaa.
         */
        private MyPriorityQueue queue = new MyPriorityQueue(100);


        @Override
        public final void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            resultVal.set(Integer.toString(sum));
            context.write(key, resultVal);
        }
    }

    /**
     *
     * @param args command line options.
     * @throws Exception execpt.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "co-occurrence");
        job.setJarByClass(CoOccurrenceDF.class);
        job.setMapperClass(CoOccurrenceDFMapper.class);
        job.setReducerClass(CoOccurrenceDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(DFDB_PATH));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }
}
