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

package test.net.broomie.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Properties;

import junit.framework.TestCase;

import net.broomie.utils.Tokenizer;

/**
 * A test case for Tokenizer class.
 * @author kimura
 */
public class TokenizerTest extends TestCase {

    /** A Tokenizer instance. */
    private Tokenizer tokenizer;

    /** A buffer for storing corpus. */
    private ArrayList<String> corpus;

    /**
     * The setup method for Junit.
     * @throws Exception Throw a Exception.
     */
    protected final void setUp() throws Exception {
        super.setUp();
        corpus = new ArrayList<String>();
        Properties prop = new Properties();
        prop.loadFromXML(new FileInputStream("conf/libnakameguro_test.xml"));
        String resource = prop.getProperty("libnakameguro.test.Tokenizer");
        try {
            BufferedReader in = new BufferedReader(
                        new InputStreamReader(new FileInputStream(resource)));
            String line;
            while ((line = in.readLine()) != null) {
                corpus.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String confPath = prop.getProperty("libnakameguro.test.Sen");
        tokenizer = new Tokenizer(confPath);
    }

    /**
     * A implement tear down method for Junit.
     * @throws Exception A exception for this method.
     */
    protected final void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * A test for Tokenizer class.
     */
    public final void testTokenize() {
        for (String line : corpus) {
            System.err.println("[Tokenizer] input:" + line);
            String[] rv =
                tokenizer.getToken(line,
                        EnumSet.of(Tokenizer.ExtractType.Noun));
            assertTrue(rv.length > 2);
        }
    }
}
