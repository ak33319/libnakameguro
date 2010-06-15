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
     * @author kimura
     *
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        /** the object for value of Mapper. */
        private static final IntWritable ONE = new IntWritable(1);

        /** the object for key of Mapper. */
        private Text word = new Text();

        /** Tokenizer instance. */
        private Tokenizer tokenizer = new Tokenizer();

        /**
        * @param key map key.
         * @param value map value.
         * @param context Context object.
         * @throws IOException exception for input error.
         * @throws InterruptedException exception.
         */
        @Override
        public final void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String buf = value.toString();
            System.err.println(buf.length());
                if (buf.length() > MAX_LINE_LENGTH) {
                    buf = buf.substring(0, MAX_LINE_LENGTH);
                }
            String[] result =
                tokenizer.getToken(buf, EnumSet.of(Tokenizer.ExtractType.Noun,
                                                   Tokenizer.ExtractType.Unk));
            for (String token : result) {
                    word.set(token);
                    context.write(word, ONE);
                }
        }
    }

    /**
     *
     * @author kimura
     *
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /** value object for store value of each key. */
        private IntWritable result = new IntWritable();

        /**
         * @param key the key for reducer.
         * @param values the values for reducer.
         * @param context context object.
         * @exception IOException exception for readind data erorr.
         * @exception InterruptedException exception.
         */
        public final void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
           }
           result.set(sum);
           context.write(key, result);
        }
    }

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
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }
}
