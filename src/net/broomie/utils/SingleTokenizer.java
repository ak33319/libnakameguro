package net.broomie.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.Properties;
import java.util.HashMap;
import java.util.Iterator;
import java.util.*;


/**
 * Tokenize for Japanese document.
 * @author kimura
 */
public final class SingleTokenizer {

    /**
     * Tokenize Japanese document.
     * @param args The arguments from command-line.
     * @throws IOException io exception.
     */
    public static void main(String[] args) throws IOException {
        GoSenTokenizer tokenizer;
        Properties prop = new Properties();
        prop.loadFromXML(new FileInputStream("conf/libnakameguro_test.xml"));
        String tokenizerConf = prop.getProperty("libnakameguro.test.GoSen");
        tokenizer = new GoSenTokenizer(tokenizerConf);
        BufferedReader in =
            new BufferedReader(new InputStreamReader(
                    new FileInputStream(args[0])));
        String line;
	HashMap<String, Integer> map = new HashMap<String, Integer>(1000000);
        while ((line = in.readLine()) != null) {
            String[] rv = tokenizer.getToken(line,
                    EnumSet.of(GoSenTokenizer.ExtractType.Noun));
            for (String token : rv) {
		if (!map.containsKey(token)) {
		    map.put(token, 1);
		} else {
		    map.put(token, map.get(token).intValue() + 1);
		}
            }
        }
	Iterator itr = map.keySet().iterator();
	while(itr.hasNext()) {
	    Object token = itr.next();
	    System.out.println(token + "\t" + map.get(token));
	}
    }
}
