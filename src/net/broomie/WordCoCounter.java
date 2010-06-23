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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.broomie.mapper.TokenizeMapper;
import net.broomie.mapper.CoCounteMapper;
import net.broomie.reducer.CoCounteReducer;
import net.broomie.reducer.TokenizeReducer;

import static net.broomie.ConstantsClass.LIB_NAKAMEGURO_CONF;
import static net.broomie.ConstantsClass.PROP_DFDB;
import static net.broomie.ConstantsClass.WORD_CO_COUNTER_REDUCER_NUM;
import static net.broomie.ConstantsClass.PROP_LINE_NUM;
/**
 * This class is Word co-occurence count for Japanese document with Map-Reduce.
 * @author kimura
 *
 */
public final class WordCoCounter extends Configured implements Tool {


    /**
     * The Constructor for WordCoCounter class.
     */
    private WordCoCounter() { }

    /** The String buffer for input file path. */
    private String in;

    /** The String buffer for output directory path on HDFS. */
    private String out;

    /** Number of arugments for this program needed. */
    private final int argNum = 3;

    /**
     * This method is implement for creating the dfdb with MapReduce.
     * @param conf Specify the conf object, which is hadoop Configuration.
     * @param dfdb Specify the dfdb directory path on HDFS.
     * @return Return `true' if success, return `false' if fail.
     * @throws IOException Exception for a input file IO.
     * @throws InterruptedException Exception for return waitForCompletion().
     * @throws ClassNotFoundException Exception for Mapper and Reduce class.
     * @throws URISyntaxException Exception for new URI().
     * The dfdb means `document frequency'.
     */
    private boolean runWordCount(Configuration conf, String dfdb)
        throws IOException, InterruptedException,
        ClassNotFoundException, URISyntaxException {
        String reducerNum = conf.get(WORD_CO_COUNTER_REDUCER_NUM);
        Job job = new Job(conf);
        job.setJarByClass(WordCoCounter.class);
        TextInputFormat.addInputPath(job, new Path(in));
        FileSystem fs = FileSystem.get(new URI(dfdb), conf);
        FileStatus[] status = fs.listStatus(new Path(dfdb));
        System.out.println(status);
        if (status != null) {
            fs.delete(new Path(dfdb), true);
        }
        fs.close();
        FileOutputFormat.setOutputPath(job, new Path(dfdb));
        job.setMapperClass(TokenizeMapper.class);
        job.setReducerClass(TokenizeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(Integer.valueOf(reducerNum));
        boolean rv = job.waitForCompletion(true);
        if (rv) {
            Counters counters = job.getCounters();
            long inputNum = counters.findCounter(
                    "org.apache.hadoop.mapred.Task$Counter",
                    "MAP_INPUT_RECORDS").getValue();
                FileSystem hdfs = FileSystem.get(conf);
                String numLinePath = conf.get(PROP_LINE_NUM);
                FSDataOutputStream stream =
                    hdfs.create(new Path(numLinePath));
                stream.writeUTF(String.valueOf((int) inputNum));
                stream.close();
        }
        return rv;
    }

    /**
     * This method is implement for counting the co-occurance with MapReduce.
     * @param conf Specify the conf object, which is hadoop Configuration.
     * @return Return `true' if success, return `false' if fail.
     * @throws IOException Exception for a input file IO.
     * @throws InterruptedException Exception for return waitForCompletion().
     * @throws ClassNotFoundException Exception for Mapper and Reduce class.
     */
    private boolean runWordCoCount(Configuration conf)
        throws IOException, InterruptedException, ClassNotFoundException {
        String reducerNum = conf.get(WORD_CO_COUNTER_REDUCER_NUM);
        Job job = new Job(conf);
        job.setJarByClass(WordCoCounter.class);
        TextInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setMapperClass(CoCounteMapper.class);
        job.setReducerClass(CoCounteReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(Integer.valueOf(reducerNum));
        return job.waitForCompletion(true);
    }

   /**
    * This method is used in order to print the usage of this program.
    */
   private void printUsage() {
       System.err.println("");
       System.err.println("WordCounter : A map/reduce program"
               + " for countting the occurrance of Japanes words.");
       System.err.println();
       System.err.println("\t[usage] hadoop jar"
               + " libnakamegruo.jar -i input -o output");
       System.err.println("\t\t-i, --input=file\tspecify the input file");
       System.err.println("\t\t-o, --output=dir\tspecify"
               + " the output directory name of HDFS");
       System.err.println();
       System.exit(-1);
   }

   /**
    * This method is used in order to get options from arguments.
    * @param args Specify the arguments from command line.
    */
   private void procArgs(final String[] args) {
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
           } else {
               printUsage();
           }
       }
   }

   /**
    * This method is in order to run the WordCoCount process.
    * @param args Specify the arguments from command line.
    * @return Return 0 if success, return 1 if fail.
    * @throws IOException Exception for a input file IO.
    * @throws InterruptedException Exception for return waitForCompletion().
    * @throws ClassNotFoundException Exception for Mapper and Reduce class.
    */
    public int run(final String[] args)
        throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        conf.addResource(LIB_NAKAMEGURO_CONF);
        procArgs(args);
        String dfdb = conf.get(PROP_DFDB);
        try {
            runWordCount(conf, dfdb);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        try {
            DistributedCache.addCacheFile(new URI(dfdb), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        runWordCoCount(conf);

        return 0;
    }

    /**
     * The main method for WordCoCounter class.
     * @param args Specify the arguments from command line.
     * @throws Exception Exception for ToolRunnner.run() method.
     */
    public static void main(final String[] args) throws Exception {
        int rv = ToolRunner.run(new WordCoCounter(), args);
        System.exit(rv);
    }
}
