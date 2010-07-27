package net.broomie.mapper;

import java.io.IOException;
import java.util.EnumSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.broomie.utils.GoSenTokenizer;
import static net.broomie.ConstantsClass.PROP_SEN_CONF;
import static net.broomie.ConstantsClass.MAX_LINE_LENGTH;
import static net.broomie.ConstantsClass.NUM_OF_AROUND_WORD;

/**
 * The Reducer class for co-counting from Japanese document.
 * @author kimura
 *
 */
public final class CoCounteMapper2
            extends Mapper<Object, Text, Text, Text> {

    /** the Text object for key of Mapper. */
    private Text targetToken = new Text();

    /** the  Text object for saving a co-occurrence words.*/
    private Text aroundToken = new Text();

    /** Tokenizer instance. */
    private GoSenTokenizer tokenizer;

    /** The regex patter for searching the part-00 files.*/
    private Pattern pattern = Pattern.compile("^[0-9]+$");

    /** The constructor for CoCounterMapper class. */
    private CoCounteMapper2() { }

    /**
     * The setup method for TokenizeMapper.
     *  This method will run before map phase.
     *  @param context Specify the hadoop Context object.
     */
    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String senConf = conf.get(PROP_SEN_CONF);
        tokenizer = new GoSenTokenizer(senConf);
    }

    /**
     * The map method for counting a co occurrence of Japanese doc.
     * @param key Specify the map key.
     * @param value Specify the map value.
     * @param context Specify the hadoop Context object.
     * @throws IOException Exception for the input file.
     * @throws InterruptedException Exception for the waiting process.
     */
    @Override public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String maxLineLengthBuf = conf.get(MAX_LINE_LENGTH);
        int maxLineLength = Integer.valueOf(maxLineLengthBuf);
        String numOfAroundWordsBuf = conf.get(NUM_OF_AROUND_WORD);
        int numOfAroundWords = Integer.valueOf(numOfAroundWordsBuf);
        String buf = value.toString();
        if (buf.length() > maxLineLength) {
            buf = buf.substring(0, maxLineLength);
        }
        buf = net.broomie.utils.Normalizer.normalize(buf);
        //String[] result = tokenizer.getToken(buf, EnumSet.of(GoSenTokenizer.ExtractType.Noun, GoSenTokenizer.ExtractType.Adj));
	tokenizer.extractToken2(buf);

	String[] nouns = tokenizer.getNoun();
	String[] adjs = tokenizer.getAdj();
	for (String noun : nouns) {
	    Matcher matcher = pattern.matcher(noun);
	    if (!matcher.matches()) {
		targetToken.set(noun);
		for (String adj : adjs) {
		    aroundToken.set(adj);
		    context.write(targetToken, aroundToken);
		}
	    }
	}
    }
}