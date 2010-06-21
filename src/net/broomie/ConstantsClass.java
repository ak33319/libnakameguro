/**
 * Constants class.
 */
package net.broomie;

/**
 *
 * @author kimura
 *
 */
public final class ConstantsClass {

    /** private constructor. */
    private ConstantsClass() { }

    /** the reading buffer size for IOUtils.copyByte. */
    public static final int READ_BUFFER_SIZE = 4096;

    /** the max length for each line(document). */
    public static final int MAX_LINE_LENGTH = 200;

    /** the flag for result. */
    public static final int ERROR_FLAG = 1;

    /** the flag for result. */
    public static final int NORMAL_FLAG = 1;

    /** libnakameguro configuration path*/
    public static final String LIB_NAKAMEGURO_CONF = "conf/libnakameguro.xml";

    /** dfdb property name */
    public static final String PROP_DFDB = "libnakameguro.dfdb";

    /** sen conf property name */
    public static final String PROP_SEN_CONF = "libnakameguro.sen";
    
    /** Num of the Reducer for WordCounter */
    public static final String WORD_COUNTER_REDUCER_NUM = "libnakameguro.WordCounterReducerNum";
}
