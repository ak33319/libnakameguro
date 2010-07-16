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

package net.broomie.utils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/** import libraries for GoSen. */
import net.java.sen.StringTagger;
import net.java.sen.SenFactory;
import net.java.sen.dictionary.Token;

/**
 *
 * @author kimura
 *
 */
public class GoSenTokenizer {

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

    /** The Tagger object for extract token from Japanese document. */
    private StringTagger tagger;

    /**
     * The constructor for Tokenizer class.
     * @param senConfPath Specify the path for GoSen configuration.
     */
    public GoSenTokenizer(String senConfPath) {
        try {
            tagger = SenFactory.getStringTagger(senConfPath);
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
            List<Token> tokens = tagger.analyze(str);
            if (tokens != null) {
                for (Token token : tokens) {
                    String pos = token.getMorpheme().toString().substring(0, 2);
                    if (pos.equals(nounDef)) {
                        if (type.contains(ExtractType.Noun)) {
                            result.add(token.getSurface());
                            }
                    } else if (pos.equals(verbDef)) {
                        if (type.contains(ExtractType.Verb)) {
                            result.add(token.getSurface());
                            }
                    } else if (pos.equals(adjDef)) {
                        if (type.contains(ExtractType.Adj)) {
                            result.add(token.getSurface());
                            }
                    } else if (pos.equals(unkDef)) {
                        if (type.contains(ExtractType.Unk)) {
                            result.add(token.getSurface());
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
