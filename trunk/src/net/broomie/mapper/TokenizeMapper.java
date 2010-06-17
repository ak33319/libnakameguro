/**
 * TokenizerMapper - hoge fuga 
 */
package net.broomie.mapper;

import static net.broomie.ConstantsClass.*;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.broomie.utils.Tokenizer;

/**
 *
 * @author kimura
 *
 */
public class TokenizeMapper
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