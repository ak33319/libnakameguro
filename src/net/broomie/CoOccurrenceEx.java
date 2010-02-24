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
public final class CoOccurrenceEx {

    /**
     * private constructor.
     */
    private CoOccurrenceEx() {
    }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccuranceExMapper extends
            Mapper<Object, Text, Text, Text> {

        /** the object for key of Mapper. */
        private Text mapKey = new Text();

        /** post word. */
        private Text mapVal = new Text();

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

            LinkedHashMap<String, LinkedHashMap<String, Integer>> coOccurrenceCnt = new LinkedHashMap<String, LinkedHashMap<String, Integer>>();

            for (int i = 0; i < resultLength; i++) {
                for (int j = 1; j <= 5; j++) {
                    if (i - j >= 0) {
                        if (!coOccurrenceCnt.containsKey(result[i])) {
                            LinkedHashMap<String, Integer> cntBuf =
                                new LinkedHashMap<String, Integer>();
                            cntBuf.put(result[i - j], 1);
                            coOccurrenceCnt.put(result[i], cntBuf);
                        } else {
                            LinkedHashMap<String, Integer> cntBuf =
                                coOccurrenceCnt.get(result[i]);
                            if (!cntBuf.containsKey(result[i - j])) {
                                cntBuf.put(result[i - j], 1);
                            } else {
                                cntBuf.put(result[i - j], cntBuf.get(
                                        result[i - j]).intValue() + 1);
                            }
                        }
                    }
                    if (i + j < resultLength) {
                        if (!coOccurrenceCnt.containsKey(result[i])) {
                            LinkedHashMap<String, Integer> cntBuf =
                                new LinkedHashMap<String, Integer>();
                            cntBuf.put(result[i + j], 1);
                        } else {
                            LinkedHashMap<String, Integer> cntBuf =
                                coOccurrenceCnt.get(result[i]);
                            if (!cntBuf.containsKey(result[i + j])) {
                                cntBuf.put(result[i + j], 1);
                            } else {
                                cntBuf.put(result[i + j], cntBuf.get(
                                        result[i + j]).intValue() + 1);
                            }
                        }
                    }
                }
            } // end of for loop.

            Iterator<String> targetTokenIter =
                coOccurrenceCnt.keySet().iterator();
            while (targetTokenIter.next() != null) {
                String targetToken = targetTokenIter.next();
                mapKey.set(targetToken);
                LinkedHashMap<String, Integer> aroundTokenBuf =
                    coOccurrenceCnt.get(targetToken);
                Iterator<String>aroundTokenIter =
                    aroundTokenBuf.keySet().iterator();
                StringBuilder resultVal = new StringBuilder();
                while (aroundTokenIter.next() != null) {
                    String aroundToken = aroundTokenIter.next();
                    Integer aroundTokenVal = aroundTokenBuf.get(aroundToken);
                    if (resultVal.length() == 0) {
                        resultVal.append(aroundToken + "\t");
                        resultVal.append(aroundTokenVal.toString());
                    } else {
                        resultVal.append("\t" + aroundToken + "\t");
                        resultVal.append(aroundTokenVal.toString());
                    }
                }
                mapVal.set(resultVal.toString());
                context.write(mapKey, mapVal);
            }
        }
    }

    /**
     *
     * @author kimura
     *
     */
    public static class CoOccuranceExReducer extends
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
            for (Text aroundWordsBuf : values) {
                String aroundWord = aroundWordsBuf.toString();
                
                /*
                if (!counter.containsKey(aroundWord)) {
                    counter.put(aroundWord, 1);
                } else {
                    counter.put(
                            aroundWord, counter.get(aroundWord).intValue() + 1);
                }
                */
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
        job.setJarByClass(CoOccurrenceEx.class);
        job.setMapperClass(CoOccuranceExMapper.class);
        // job.setCombinerClass(CoOccuranceReducer.class);
        job.setReducerClass(CoOccuranceExReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }

}
