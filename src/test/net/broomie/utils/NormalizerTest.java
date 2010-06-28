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

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Test case for Nomalizer class.
 * @author kimura
 */
public class NormalizerTest extends TestCase {

    /** Buffer for the corpus data. */
    private ArrayList<String> corpus;

    /**
     * setup.
     * @throws Exception exception.
     */
    protected final void setUp()  throws Exception {
        super.setUp();
        corpus = new ArrayList<String>();
        Properties prop = new Properties();
        prop.loadFromXML(new FileInputStream("conf/libnakameguro_test.xml"));
        String resource = prop.getProperty("libnakameguro.test.Normalizer");
        try {
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(
                            new FileInputStream(resource)));
            String line;
            while ((line = in.readLine()) != null) {
                corpus.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implements tearDown.
     * @throws Exception exception.
     */
    protected final void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * A test for normalize().
     */
    public final void testNormalizer() {
        for (int i = 0; i < corpus.size() - 1; i++) {
            String testData = corpus.get(i);
            String correctData = corpus.get(++i);
            String rv = net.broomie.utils.Normalizer.normalize(testData);
            System.err.println("[Normalizer] " + correctData + " <=> " + rv);
            assertTrue(rv.equals(correctData));
        }
    }
}
