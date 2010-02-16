

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

public class WriteDocumment {
	public static void main(String args[]) throws IOException {
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
		//IOUtils.copyBytes(in, out, 4096, true);
		IOUtils.copyBytes(in, out, 128, true);
	}
}
