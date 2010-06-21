/**
 * ReadData - the class for reading data from hdfs.
 */
package net.broomie.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import static net.broomie.ConstantsClass.READ_BUFFER_SIZE;

/**
 *
 * @author kimura
 *
 */
public final class ReadData {

    /**
     * private constructor.
     */
    private ReadData() { }

    /**
     *
     * @param args - arguments from command line.
     * @throws IOException - Exception of reading file.
     */
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        String bufferSizeBuf = conf.get(READ_BUFFER_SIZE);
        int readBufferSize = Integer.valueOf(bufferSizeBuf);
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, out, readBufferSize, false);
        } finally {
            IOUtils.closeStream(in);
        }
        System.out.println(out);
    }
}
