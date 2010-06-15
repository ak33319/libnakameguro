package net.broomie.lib;

import java.util.ArrayList;
import java.util.EnumSet;

import net.java.sen.StringTagger;
import net.java.sen.Token;

import static net.broomie.ConstantsClass.*;

/**
 *
 * @author kimura
 *
 */
public class Tokenizer {

    /** Noun string definition. */
    private final String nounDef = "名詞";

    /** Verb string definition. */
    private final String verbDef = "動詞";

    /** Adjective string definition. */
    private final String adjDef = "形容";

    /** Unknown string definition. */
    private final String unkDef = "未知";

    /**
     *
     * @author kimura
     *
     */
    public static enum ExtractType {
        /** Noun type definition. */
        Noun,
        /** Verb type definition. */
        Verb,
        /** Adjective type definition .*/
        Adj,
        /** Unknown type definition. */
        Unk,
    }

    /** tagger object for extract token from Japanese document. */
    private StringTagger tagger;

    /**
     * Constructor for Tokenizer.
     */
    public Tokenizer() {
        try {
            tagger = StringTagger.getInstance(SEN_CONF_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param str the string object for extracting tokens.
     * @param type the extracting type. Tokenizer.Noun etc...
     * @return tokens.
     */
    public final String[] getToken(String str, EnumSet<ExtractType> type) {
        ArrayList<String> result = new ArrayList<String>();

        try {
            Token[] token = tagger.analyze(str);
            if (token != null) {
                for (int i = 0; i < token.length; i++) {
                    String pos = token[i].getPos().substring(0, 2);
                    if (pos.equals(nounDef)) {
                        if (type.contains(ExtractType.Noun)) {
                            result.add(token[i].getBasicString());
                        }
                    } else if (pos.equals(verbDef)) {
                        if (type.contains(ExtractType.Verb)) {
                            result.add(token[i].getBasicString());
                        }
                    } else if (pos.equals(adjDef)) {
                        if (type.contains(ExtractType.Adj)) {
                            result.add(token[i].getBasicString());
                        }
                    } else if (pos.equals(unkDef)) {
                        if (type.contains(ExtractType.Unk)) {
                            result.add(token[i].getBasicString());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return (String[]) result.toArray(new String[0]);
    }
}
