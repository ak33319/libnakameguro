/**
 * CoCounterMapper - hoge fuga 
 */
package net.broomie.mapper;

import static net.broomie.ConstantsClass.*;

import java.io.IOException;
import java.util.EnumSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.broomie.utils.Tokenizer;
import net.broomie.utils.Normalizer;

/**
 *
 * @author kimura
 *
 */
public class CoCounteMapper
    extends Mapper<Object, Text, Text, Text> {

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
        buf = net.broomie.utils.Normalizer.normalize(buf);
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

