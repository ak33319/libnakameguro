/**
 * CoOccurance - co-occura word count class.
 */
package net.broomie;

import java.io.IOException;

import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static net.broomie.ConstantsClass.*;
import java.util.EnumSet;
import java.util.Set;
import java.util.Iterator;
import java.lang.StringBuilder;

/**
 * 
 * @author kimura
 * 
 */
public final class CoOcurance {

    /**
     * private constructor.
     */
    private CoOcurance() {
    }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccuranceMapper extends
            Mapper<Object, Text, Text, Text> {

        /** the object for key of Mapper. */
        private Text targetToken = new Text();

        /** post word. */
        private Text aroundToken = new Text();

        /** Tokenizer instance. */
        private Tokenizer tokenizer = new Tokenizer();

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
                    targetToken.set(result[i]);
                for (int j = 1; j <= 5; j++) {
                    if (i - j >= 0) {
                        aroundToken.set(result[i - j]);
                        context.write(targetToken, aroundToken);
                    }

                    if (i + j < resultLength) {
                        aroundToken.set(result[i + j]);
                        context.write(targetToken, aroundToken);
                    }
                }

            }
            /*
            String preWord = "";
            for (String postWord : result) {

                if (preWord.length() > 0) {
                    preToken.set(preWord);
                    postToken.set(postWord);
                    context.write(preToken, postToken);
                    // context.write(postToken, preToken);

                }
                preWord = postWord;
            }
            */
        }

    }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccuranceReducer extends
            Reducer<Text, Text, Text, Text> {

        /**
         * @param key
         *            the key for reducer.
         * @param values
         *            the values for reducer.
         * @param context
         *            context object.
         * @exception IOException
         *                exception for readind data erorr.
         * @exception InterruptedException
         *                exception.
         */
        public final void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            LinkedHashMap<String, Integer> counter =
                new LinkedHashMap<String, Integer>(1000);
            System.err.println("KEY:" + key);
            for (Text wordBuf : values) {
                String word = wordBuf.toString();
                System.err.println(word);
                if (!counter.containsKey(word)) {
                    counter.put(word, 1);
                } else {
                    counter.put(word, counter.get(word).intValue() + 1);
                }
            }
            Set<String> tokensSet = counter.keySet();
            Iterator<String> itr = tokensSet.iterator();
            StringBuilder result = new StringBuilder();
            while (itr.hasNext()) {
                String token = (String) itr.next();
                Integer valBuf = (Integer) counter.get(token);
                result.append(token + ":" + valBuf + "\t");
            }
            Text resultVal = new Text(result.toString());
            context.write(key, resultVal);
        }
    }

    /**
     *
     * @param args
     *            arguments from command line.
     * @throws Exception
     *             exception.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(CoOcurance.class);
        job.setMapperClass(CoOccuranceMapper.class);
        //job.setCombinerClass(CoOccuranceReducer.class);
        job.setReducerClass(CoOccuranceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }

}
