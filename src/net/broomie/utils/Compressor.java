package net.broomie.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class Compressor extends Configured implements Tool {

    public static class CompressorMapper
        extends Mapper<Object, Text, Text, NullWritable> {

        private final NullWritable nullVal = NullWritable.get();

        @Override
        public final void map(Object key, Text value, Context context) {
            try {
                context.write(value, nullVal);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private final int argNum = 3;

    private String in;

    private String out;

    private String codec;

    private void printUsage() {
        System.err.println("");
        System.err.println("Compressor : A compressor for data on HDFS");
        System.err.println();
        System.err.println("\t[usage] hadoop jar net.broomie.libnakameguro.utils.Compressor -i input -o output -c codec");
        System.err.println("\t-i, --input=file\tspecify the input file on HDFS.");
        System.err.println("\t-o, --output=directory\tspecify the output dir on HDFS.");
        System.err.println("\t-c, --codec=type\tspecify the codec type.");
        System.err.println("\tcodec type = [deflate | gzip | bzip2 | lzo]");
        System.err.println();
        System.exit(-1);
    }
    
    private void procArgs(final String[] args) {
        System.err.println("length:" + args.length);
        if (args.length < argNum) {
            printUsage();
        }
        for (int i = 0; i < args.length; i++) {
            String elem = args[i];
            if (elem.equals("-i")) {
                in = args[++i];
            } else if (elem.matches("^--input=.*")) {
                int idx = elem.indexOf("=");
                in = elem.substring(idx + 1, elem.length());
            } else if (elem.equals("-o")) {
                out = args[++i];
            } else if (elem.matches("^--output=.*")) {
                int idx = elem.indexOf("=");
                out = elem.substring(idx + 1, elem.length());
            } else if (elem.equals("-c")) {
                codec = args[++i];
            } else if (elem.matches("^--codec=.*")) {
                int idx = elem.indexOf("=");
                codec = elem.substring(idx + 1, elem.length());
            } else {
                printUsage();
            }
        }
    }

    private boolean runCompressor(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        //conf.set
        //conf.setMapOutPutComressorClass(GzipCodec.class);
        Job job = new Job(conf);
        job.setJarByClass(Compressor.class);
        TextInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setMapperClass(CompressorMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true);
    }

    public int run(final String[] args) {
        procArgs(args);
        if (in == null || out == null) {
            printUsage();
        }
        Configuration conf = getConf();
        boolean rvBuf = true;
        try {
            rvBuf = runCompressor(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int rv = rvBuf ? 0 : 1;
        return rv;
    }

    public static void main(final String[] args) throws Exception {
        int rv = ToolRunner.run(new Compressor(), args);
        System.exit(rv);
    }
}
