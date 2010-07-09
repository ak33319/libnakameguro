package net.broomie.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.Properties;



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
        Tokenizer tokenizer;
        Properties prop = new Properties();
        prop.loadFromXML(new FileInputStream("conf/libnakameguro_test.xml"));
        String tokenizerConf = prop.getProperty("libnakameguro.test.Tokenizer");
        tokenizer = new Tokenizer(tokenizerConf);
        BufferedReader in =
            new BufferedReader(new InputStreamReader(
                    new FileInputStream(args[0])));
        String line;
        while ((line = in.readLine()) != null) {
            String[] rv = tokenizer.getToken(line,
                    EnumSet.of(Tokenizer.ExtractType.Noun));
            for (String token : rv) {
                System.out.println(token);
            }
        }
    }
}
