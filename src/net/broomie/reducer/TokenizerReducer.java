/**
 * TokenizerReducer - hoge fuga
 */
package net.broomie.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author kimura
 *
 */
public class TokenizerReducer
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
