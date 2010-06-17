/**
 * CoCounterReducer - hoge fuga 
 */
package net.broomie.reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.broomie.utils.MyPriorityQueue;

/**
 *
 * @author kimura
 *
 */
public class CoCounteReducer extends
               Reducer<Text, Text, Text, Text> {

    /** tf-idf map. */
    private LinkedHashMap<String, Integer> wordCount =
        new LinkedHashMap<String, Integer>(100000);

    @Override
    public final void setup(Context context) {
        Configuration conf = context.getConfiguration();
        conf.addResource("conf/libnakameguro.xml");
        try {
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (cacheFiles != null) {
                for (Path cachePath : cacheFiles) {
                    loadCacheFile(cachePath, context);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static FilenameFilter getFileRegexFilter(String regex) {
        final String regex_ = regex;
        return new FilenameFilter() {
            public boolean accept(File file, String name) {
                boolean ret = name.matches(regex_);
                return ret;
           }
        };
    }

    /**
     *
     * @param cachePath cache file path
     * @throws IOException io exception,
     */

    private final void loadCacheFile(Path cachePath, Context context) throws IOException {

        File cacheFile = new File(cachePath.toString());
        if (cacheFile.isDirectory() == true) {
            File[] caches = cacheFile.listFiles(getFileRegexFilter("part-.*-[0-9]*"));
            for (File cache : caches) {
                BufferedReader wordReader = new BufferedReader(new FileReader(cache.getAbsolutePath()));
                String line;
                while ((line = wordReader.readLine()) != null) {
                    String[] wordCountBuf = line.split("\t");
                    wordCount.put(wordCountBuf[0], Integer.valueOf(wordCountBuf[1]));
                }
            }
        } else {
            BufferedReader wordReader = new BufferedReader(new FileReader(cachePath.toString()));
            String line;
            while ((line = wordReader.readLine()) != null) {
                String[] wordCountBuf = line.split("\t");
                wordCount.put(wordCountBuf[0], Integer.valueOf(wordCountBuf[1]));
            }
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
            String aroundWord = aroundWordsItr.next();
            if (!aroundWord.equals(key.toString())) {
                double score = counter.get(aroundWord);
                if (wordCount.containsKey(aroundWord)) {
                    score = score / Math.pow(wordCount.get(aroundWord) + 10.0, 0.8);
                    queue.add(aroundWord, score);
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