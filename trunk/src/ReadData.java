
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadData {

public static void main(String[] args) throws IOException {
String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
        in = fs.open(new Path(uri));
        // IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.copyBytes(in, out, 4096, false);
    } finally {
        IOUtils.closeStream(in);
    }
    System.out.println(out);
    }
}
