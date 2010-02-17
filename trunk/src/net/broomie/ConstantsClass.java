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

    /** the configuration path for Sen. */
    public static final String SEN_CONF_PATH = "/usr/local/sen/conf/sen.xml";
}
