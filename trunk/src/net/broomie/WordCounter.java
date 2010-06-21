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

/**
 * WordCounter - Word count for Japanese document with Map-Reduce.
 */
package net.broomie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static net.broomie.ConstantsClass.LIB_NAKAMEGURO_CONF;
import static net.broomie.ConstantsClass.WORD_COUNTER_REDUCER_NUM;
import net.broomie.mapper.TokenizeMapper;
import net.broomie.reducer.TokenizeReducer;

/**
 *
 * @author kimura
 *
 */
public final class WordCounter extends Configured implements Tool {

    /** The String buffer for input file path. */
    private String in;

    /** The String buffer for output directory path on HDFS. */
    private String out;

    /** num of arugments for this program needed. */
    private final int argNum = 3;

    /**
     * This method is used implement for thw running the word count MapReduce.
     * @param conf Specify the conf object, which is hadoop Configuration.
     * @return Return `true' if success, return `false' if fail.
     * @throws IOException Exception for a input file IO.
     * @throws InterruptedException Exception for return waitForCompletion().
     * @throws ClassNotFoundException Exception for Mapper and Reduce class.
     */
    private boolean runWordCount(Configuration conf)
        throws IOException, InterruptedException, ClassNotFoundException {
        String reducerNum = conf.get(WORD_COUNTER_REDUCER_NUM);
        Job job = new Job(conf);
        job.setJarByClass(WordCounter.class);
        TextInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setMapperClass(TokenizeMapper.class);
        job.setReducerClass(TokenizeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
     * The constructor for this class.
     */
    private WordCounter() { }

    /**
     * This method is in order to run the WordCount process.
     * @param args Specify the arguments from command line.
     * @return Return 0 if success, return 1 if fail.
     * @throws IOException Exception for a input file IO.
     * @throws InterruptedException Exception for return waitForCompletion().
     * @throws ClassNotFoundException Exception for Mapper and Reduce class.
     */
    public int run(final String[] args)
    throws IOException, InterruptedException, ClassNotFoundException {
    procArgs(args);
        if (in == null || out == null) {
            printUsage();
        }
        Configuration conf = getConf();
        conf.addResource(LIB_NAKAMEGURO_CONF);
        boolean rvBuf = runWordCount(conf);
        int rv = rvBuf  ? 0 : 1;
        return rv;
    }

    /**
     * The main method for WordCounter class.
     * @param args Specify the arguments from command line.
     * @throws Exception Exception for ToolRunnner.run() method.
     */
    public static void main(final String[] args) throws Exception {
        int rv = ToolRunner.run(new WordCounter(), args);
        System.exit(rv);
    }
}
