package net.broomie;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import static net.broomie.ConstantsClass.*;

/**
 *
 * @author kimura
 *
 */
public final class WriteDocumment {

    /**
     * private constructor.
     */
    private WriteDocumment() {
    }

    /**
     *
     * @param args
     *            arguments from command line.
     * @throws IOException
     *             exception for read data error.
     */
    public static void main(String[] args) throws IOException {

        String localSrc = args[0];
        String dst = args[1];
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(localSrc));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, READ_BUFFER_SIZE, true);
    }
}
