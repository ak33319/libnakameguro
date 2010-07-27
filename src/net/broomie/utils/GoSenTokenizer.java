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

    private ArrayList<String> nounArray;

    private ArrayList<String> verbArray;

    private ArrayList<String> adjArray;

    private ArrayList<String> unkArray;

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

	nounArray = new ArrayList<String>(100);
	verbArray = new ArrayList<String>(100);
	adjArray = new ArrayList<String>(100);
	unkArray = new ArrayList<String>(100);
    }

    private final void clear() {
	nounArray.clear();
	verbArray.clear();
	adjArray.clear();
	unkArray.clear();
    }

    public final void extractToken(String str) {
	clear();
	try {
	    List<Token> tokens = tagger.analyze(str);
	    if (tokens != null) {
		for (Token token : tokens) {
		    String pos = token.getMorpheme(). toString().substring(0, 2);
		    if (pos.equals(nounDef)) {
			nounArray.add(token.getSurface());
                    } else if (pos.equals(verbDef)) {
			verbArray.add(token.getSurface());
                    } else if (pos.equals(adjDef)) {
			adjArray.add(token.getSurface());
                    } else if (pos.equals(unkDef)) {
			unkArray.add(token.getSurface());
                    }
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private final String createCompoundNoun(ArrayList<String> nouns) {
	StringBuilder cmpNoun = new StringBuilder();
	for (String noun : nouns) {
	    cmpNoun.append(noun);
	}
	return cmpNoun.toString();
    }

    public final void extractToken2(String str) {
	clear();
	ArrayList<String> compoundNoun = new ArrayList<String>();
	try {
	    List<Token> tokens = tagger.analyze(str);
	    if (tokens != null) {
		for (Token token : tokens) {
		    String pos = token.getMorpheme().toString().substring(0, 2);
		    String baseToken = token.getMorpheme().getBasicForm();
		    if (pos.equals(nounDef)) {
			compoundNoun.add(baseToken);
                    } else if (pos.equals(verbDef)) {
			verbArray.add(baseToken);
			if (compoundNoun.size() > 0) {
			    nounArray.add(createCompoundNoun(compoundNoun));
			    compoundNoun.clear();
			}
                    } else if (pos.equals(adjDef)) {
			adjArray.add(baseToken);
			if (compoundNoun.size() > 0) {
			    nounArray.add(createCompoundNoun(compoundNoun));
			    compoundNoun.clear();
			}
                    } else if (pos.equals(unkDef)) {
			unkArray.add(baseToken);
			if (compoundNoun.size() > 0) {
			    nounArray.add(createCompoundNoun(compoundNoun));
			    compoundNoun.clear();
			}
                    } else {
			if (compoundNoun.size() > 0) {
			    nounArray.add(createCompoundNoun(compoundNoun));
			    compoundNoun.clear();
			}
		    }
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public final String[] getNoun() {
	return (String[]) nounArray.toArray(new String[0]);
    }

    public final String[] getVerb() {
	return (String[]) verbArray.toArray(new String[0]);
    }

    public final String[] getAdj() {
	return (String[]) adjArray.toArray(new String[0]);
    }

    public final String[] getUnk() {
	return (String[]) unkArray.toArray(new String[0]);
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

    public final String[] getCompoundNoun(String str) {
        ArrayList<String> result = new ArrayList<String>();
        ArrayList<String> nouns = new ArrayList<String>();
        try {
            List<Token> tokens = tagger.analyze(str);
            if (tokens != null) {
                for (Token token : tokens) {
                    String pos = token.getMorpheme().toString().substring(0, 2);
                    if (pos.equals(nounDef)) {
                        nouns.add(token.getSurface());
                    } else {
                        if (nouns.size() > 0) {
                            StringBuilder nounBuf = new StringBuilder();
                            for (String noun : nouns) {
                                nounBuf.append(noun);
                            }
                            result.add(nounBuf.toString());
                            nouns.clear();
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
