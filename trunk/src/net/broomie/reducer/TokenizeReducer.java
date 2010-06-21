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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class for tokenizing a Japanese document.
 * @author kimura
 *
 */
public class TokenizeReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    /** value object for store value of each key. */
    private IntWritable result = new IntWritable();

    /**
     * @param key the key for reducer.
     * @param values the values for reducer.
     * @param context context object.
     * @exception IOException exception for reading data error.
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
