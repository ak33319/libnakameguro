/**
* Copyright 2010 Shunya KIMURA <brmtrain@gmail.com>
*
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
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
 * The reduce class for counting co-occurrence for Japanese sentence.
 * @author kimura
 */
public class CoCounteReducer extends Reducer<Text, Text, Text, Text> {

    /** The Text object for saving each value.*/
    private Text val = new Text();

    /** The Initial hash map size for wordCount. */
    private final int hashMapInitSiz = 100000;

    /** The Initial queue size.*/
    private final int queueInitSiz = 100;

    /** The HashMap for tf-idf map. */
    private LinkedHashMap<String, Integer> wordCount =
        new LinkedHashMap<String, Integer>(hashMapInitSiz);

    /**
     * This method is in order to select part-file with regular expression.
     * @param regex Specify the regular expression.
     * @return FilenameFilter object.
     */
    private FilenameFilter getFileRegexFilter(String regex) {
        final String regex_ = regex;
        return new FilenameFilter() {
            public boolean accept(File file, String name) {
                boolean ret = name.matches(regex_);
                return ret;
           }
        };
    }

    /**
     * This method is used in order to DistributedCache files.
     * @param cachePath Specify the cache file path.
     * @param context Specify the hadoop Context object.
     * @throws IOException Exception for the DistributedCache file open.
     */
    private void loadCacheFile(Path cachePath, Context context)
        throws IOException {
        File cacheFile = new File(cachePath.toString());
        if (cacheFile.isDirectory()) {
            File[] caches =
                cacheFile.listFiles(getFileRegexFilter("part-.*-[0-9]*"));
            for (File cache : caches) {
                BufferedReader wordReader =
                    new BufferedReader(new FileReader(cache.getAbsolutePath()));
                String line;
                while ((line = wordReader.readLine()) != null) {
                    String[] wordCountBuf = line.split("\t");
                    wordCount.put(wordCountBuf[0],
                            Integer.valueOf(wordCountBuf[1]));
                }
            }
        } else {
            BufferedReader wordReader =
                new BufferedReader(new FileReader(cachePath.toString()));
            String line;
            while ((line = wordReader.readLine()) != null) {
                String[] wordCountBuf = line.split("\t");
                wordCount.put(wordCountBuf[0],
                        Integer.valueOf(wordCountBuf[1]));
            }
        }
    }

    /**
     * The setup method for TokenizeReducer.
     * This method will run before reduce phase.
     * @param context Specify the hadoop Context object.
     */
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

    /**
     * @param key Specify the key for reducer.
     * @param values Specify the values for reducer.
     * @param context Specify the hadoop Context object.
     * @exception IOException Exception for open input file.
     * @exception InterruptedException exception.
     */
    public final void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        LinkedHashMap<String, Double> counter =
            new LinkedHashMap<String, Double>(hashMapInitSiz);
        for (Text wordBuf : values) {
            String word = wordBuf.toString();
            if (!counter.containsKey(word)) {
                counter.put(word, 1.0);
            } else {
                counter.put(word, counter.get(word).intValue() + 1.0);
            }
        }
        MyPriorityQueue queue = new MyPriorityQueue(queueInitSiz);
        Iterator<String> aroundWordsItr = counter.keySet().iterator();
        while (aroundWordsItr.hasNext()) {
            String aroundWord = aroundWordsItr.next();
            if (!aroundWord.equals(key.toString())) {
                double score = counter.get(aroundWord);
                if (wordCount.containsKey(aroundWord)) {
                    score =
                        score / Math.pow(wordCount.get(aroundWord) + 10.0, 0.8);
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
