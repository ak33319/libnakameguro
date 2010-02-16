
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.EnumSet;

import net.java.sen.StringTagger;
import net.java.sen.Token;

public class Tokenizer {

    private final String NounDef = "名詞";
    private final String VerbDef = "動詞";
    private final String AdjDef = "形容";

    public static enum ExtractType {
        Noun, Verb, Adj,
    }

    private StringTagger tagger;

    private LinkedHashMap<String, Integer> words;

    public Tokenizer() {
        try {
            tagger = StringTagger.getInstance("/usr/local/sen/conf/sen.xml");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public final String[] getToken(String str, EnumSet<ExtractType> type) {
        ArrayList<String> result = new ArrayList<String>();

        try {
            Token[] token = tagger.analyze(str);

            if (token != null) {
                for (int i = 0; i < token.length; i++) {
                    String pos = token[i].getPos().substring(0, 2);
                    if (pos.equals(NounDef)) {
                        if (type.contains(ExtractType.Noun)) {
                            result.add(token[i].getBasicString());
                        }
                    } else if (pos.equals(VerbDef)) {
                        if (type.contains(ExtractType.Verb)) {
                            result.add(token[i].getBasicString());
                        }
                    } else if (pos.equals(AdjDef)) {
                        if (type.contains(ExtractType.Adj)) {
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
