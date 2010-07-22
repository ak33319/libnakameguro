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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import static net.broomie.ConstantsClass.PROP_LINE_NUM;
import static net.broomie.ConstantsClass.LIB_NAKAMEGURO_CONF;
/**
 * The reduce class for counting co-occurrence for Japanese sentence.
 * @author kimura
 */
public class TokenizeReducerTFIDF
    extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    /** The Text object for saving each value.*/
    private DoubleWritable val = new DoubleWritable();

    /** The Initial hash map size for wordCount. */
    private final int hashMapInitSiz = 100000;

    /** Number of the line for input file. */
    private double lineNum = 0.0;

    /** The HashMap for tf-idf map. */
    private LinkedHashMap<String, Integer> wordCount =
        new LinkedHashMap<String, Integer>(hashMapInitSiz);

    /**
     * This method is in order to select part-file with regular expression.
     * @param regex Specify the regular expression.
     * @return FilenameFilter object.
     */
    private FilenameFilter getFileRegexFilter(String regex) {
        final String regexBuf = regex;
        return new FilenameFilter() {
            public boolean accept(File file, String name) {
                boolean ret = name.matches(regexBuf);
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
        Configuration conf = context.getConfiguration();
        FileSystem hdfs = FileSystem.get(conf);
        String numLinePath = conf.get(PROP_LINE_NUM);
        FSDataInputStream dis = hdfs.open(new Path(numLinePath));
        String lineNumBuf = dis.readUTF();
        lineNum = (double) Integer.parseInt(lineNumBuf);
    }

    /**
     * The setup method for TokenizeReducer.
     * This method will run before reduce phase.
     * @param context Specify the hadoop Context object.
     */
    @Override
    public final void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String resourcePath = conf.get(LIB_NAKAMEGURO_CONF);
        conf.addResource(resourcePath);
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
    public final void reduce(Text key, Iterable<IntWritable> values,
            Context context)
        throws IOException, InterruptedException {
        LinkedHashMap<String, Integer> counter =
            new LinkedHashMap<String, Integer>(hashMapInitSiz);
        String keyToken = key.toString();
        for (IntWritable valBuf : values) {
            if (!counter.containsKey(keyToken)) {
                counter.put(keyToken, valBuf.get());
            } else {
                counter.put(keyToken,
                        counter.get(keyToken).intValue() + valBuf.get());
            }
        }
        Iterator<String> counterItr = counter.keySet().iterator();
        while (counterItr.hasNext()) {
            String token = counterItr.next();
            double tf = counter.get(token);
            if (wordCount.containsKey(token)) {
            int df = wordCount.get(token);
            double score = tf * Math.log(lineNum / df);
            val.set(score);
            context.write(new Text(token), val);
            }
        }
    }
}
