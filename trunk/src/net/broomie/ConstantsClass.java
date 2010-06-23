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
package net.broomie;

/**
 * This class is just a gathering constants variable.
 * @author kimura
 *
 */
public final class ConstantsClass {

    /** private constructor. */
    private ConstantsClass() { }

    /** the reading buffer size for IOUtils.copyByte. */
    public static final String READ_BUFFER_SIZE =
            "libnakameguro.readbuffersize";

    /** the max length for each line(document). */
    public static final String MAX_LINE_LENGTH = "libnakameguro.maxlinelength";

    /** the flag for result. */
    public static final int ERROR_FLAG = 1;

    /** the flag for result. */
    public static final int NORMAL_FLAG = 1;

    /** libnakameguro configuration path. */
    public static final String LIB_NAKAMEGURO_CONF = "conf/libnakameguro.xml";

    /** The name of property for dfdb path of HDFS. */
    public static final String PROP_DFDB = "libnakameguro.dfdb";

    /** The name of property for lineNum path of HDFS. */
    public static final String PROP_LINE_NUM = "libnakameguro.lineNum";

    /** The name of property for sen.conf. */
    public static final String PROP_SEN_CONF = "libnakameguro.sen";

    /** Number of the Reducer for WordCounter. */
    public static final String WORD_COUNTER_REDUCER_NUM =
        "libnakameguro.WordCounterReducerNum";

    /** Number of the Reducer for WordCoCOunter. */
    public static final String WORD_CO_COUNTER_REDUCER_NUM =
        "libnakameguro.WordCoCounterReducerNum";

    /** Number of the around word for counting  co occurrence. */
    public static final String NUM_OF_AROUND_WORD =
        "libnakameguro.numofaroundword";
}
