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
import org.apache.hadoop.filecache.DistributedCache;


import static net.broomie.ConstantsClass.*;

import java.util.EnumSet;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.regex.*;

/**
 *
 * @author kimura
 *
 */
public final class CoOccurrence {

    public static String normalize(String str) {
        StringBuilder buf = new StringBuilder(str);
        for (int i = 0; i < buf.length(); i++) {
            char c = buf.charAt(i);
            // a - z
            if (c >= 65345 && c <= 65370) {
                buf.setCharAt(i, (char) (c - 65345 + 97));
                // A - Z
            } else if (c >= 65313 && c <= 65538) {
                buf.setCharAt(i, (char) (c - 65313 + 65));
                //  0 - 9
            } else if (c >= 65296 && c <= 65305) {
                buf.setCharAt(i, (char) (c - 65296 + 48));
            }
        }

        return buf.toString().toLowerCase();
    }



    /**
     * private constructor.
     */
    private CoOccurrence() {
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

        private Pattern pattern = Pattern.compile("^[0-9]+$");

        @Override
        public final void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String buf = value.toString();
            if (buf.length() > MAX_LINE_LENGTH) {
                buf = buf.substring(0, MAX_LINE_LENGTH);
            }
            buf = normalize(buf);
            String[] result = tokenizer.getToken(buf, EnumSet
                    .of(Tokenizer.ExtractType.Noun, Tokenizer.ExtractType.Unk));

            int resultLength = result.length;

            for (int i = 0; i < resultLength; i++) {
                Matcher matcher = pattern.matcher(result[i]);
                if ((matcher.matches()) == false) {
                    targetToken.set(result[i]);
                    for (int j = 1; j <= 10; j++) {
                        if (i - j >= 0) {
                            matcher = pattern.matcher(result[i - j]);
                            if ((matcher.matches()) == false ) {
                                aroundToken.set(result[i - j]);
                                context.write(targetToken, aroundToken);
                            }
                        }
                        if (i + j < resultLength) {
                            matcher = pattern.matcher(result[i + j]);
                            if ((matcher.matches()) == false ) {
                                aroundToken.set(result[i + j]);
                                context.write(targetToken, aroundToken);
                            }
                        }
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
    public static class CoOccuranceReducer extends
            Reducer<Text, Text, Text, Text> {

        /** tf-idf map. */
        private LinkedHashMap<String, Integer> wordCount =
            new LinkedHashMap<String, Integer>(100000);

        @Override
        public final void setup(Context context) {
            try {
                String dfDBCacheName = new Path(DFDB_PATH).getName();
                Path[] cacheFiles =
                    DistributedCache.getLocalCacheFiles(
                            context.getConfiguration());
                if (cacheFiles != null) {
                    for (Path cachePath : cacheFiles) {
                        if (cachePath.getName().equals(dfDBCacheName)) {
                            loadCacheFile(cachePath);
                        }
                    }
                }
            } catch (IOException e) {

            }
        }

        /**
         *
         * @param cachePath cache file path
         * @throws IOException io exception,
         */
        final void loadCacheFile(Path cachePath) throws IOException {
            BufferedReader wordReader =
                new BufferedReader(new FileReader(cachePath.toString()));
            try {
                String line;
                System.out.println("start read");
                while ((line = wordReader.readLine()) != null) {
                    System.out.println(line);
                    String[] wordCountBuf = line.split("\t");
                    wordCount.put(wordCountBuf[0],
                            Integer.valueOf(wordCountBuf[1]));
                }
                System.out.println("end read");
            } finally {
                System.out.println("open erro");
                wordReader.close();
            }
        }

        /**
         * val.
         */
        private Text val = new Text();

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

            LinkedHashMap<String, Double> counter =
                new LinkedHashMap<String, Double>(1000);
            for (Text wordBuf : values) {
                String word = wordBuf.toString();
                if (!counter.containsKey(word)) {
                    counter.put(word, 1.0);
                } else {
                    counter.put(word, counter.get(word).intValue() + 1.0);
                }
            }


            MyPriorityQueue queue = new MyPriorityQueue(100);
            Iterator<String> aroundWordsItr = counter.keySet().iterator();
            while (aroundWordsItr.hasNext()) {
                String word = aroundWordsItr.next();
                if (!word.equals(key.toString())) {
                    double score = counter.get(word);
                    if (wordCount.containsKey(word)) {
                        score =
                            score / Math.pow(wordCount.get(word) + 10.0, 0.8);
                        queue.add(word, score);
                    }
                }
            }

            StringBuilder resultVal = new StringBuilder();
            MyPriorityQueue.Entity ent;
            while ((ent = queue.poll()) != null) {
                if (resultVal.length() > 0) {
                    resultVal.insert(0, "\t");
                }
                String resultScore = String.format("%.2f", ent.getVal());
                resultVal.insert(0, ent.getKey() + ":" + resultScore);
            }
            val.set(resultVal.toString());
            context.write(key, val);
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

        DistributedCache.addCacheFile(new Path(DFDB_PATH).toUri(), conf);
        Job job = new Job(conf, "word count");
        job.setJarByClass(CoOccurrence.class);
        job.setMapperClass(CoOccuranceMapper.class);
        //job.setCombinerClass(CoOccuranceReducer.class);
        job.setReducerClass(CoOccuranceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? NORMAL_FLAG : ERROR_FLAG);
    }
}