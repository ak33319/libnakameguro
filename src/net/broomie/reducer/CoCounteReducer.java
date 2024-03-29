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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.broomie.utils.MyPriorityQueue;
import static net.broomie.ConstantsClass.LIB_NAKAMEGURO_CONF;
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

        LinkedHashMap<String, Integer> counter =
            new LinkedHashMap<String, Integer>(hashMapInitSiz);
        for (Text wordBuf : values) {
            String word = wordBuf.toString();
            if (!counter.containsKey(word)) {
                counter.put(word, 1);
            } else {
                counter.put(word, counter.get(word).intValue() + 1);
            }
        }
        MyPriorityQueue queue = new MyPriorityQueue(queueInitSiz);
        Iterator<String> aroundWordsItr = counter.keySet().iterator();
        while (aroundWordsItr.hasNext()) {
            String aroundWord = aroundWordsItr.next();
            if (!aroundWord.equals(key.toString())) {
                int tf = counter.get(aroundWord);
                queue.add(aroundWord, tf);
            }
        }
        StringBuilder resultVal = new StringBuilder();
        MyPriorityQueue.Entity ent;
        while ((ent = queue.poll()) != null) {
            if (resultVal.length() > 0) {
                resultVal.insert(0, "\t");
            }
            String resultScore = String.format("%d", (int) ent.getVal());
            resultVal.insert(0, ent.getKey() + ":" + resultScore);
        }
        val.set(resultVal.toString());
        context.write(key, val);
    }
}
