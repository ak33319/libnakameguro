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
package net.broomie.mapper;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.broomie.utils.Tokenizer;
import static net.broomie.ConstantsClass.PROP_SEN_CONF;
import static net.broomie.ConstantsClass.MAX_LINE_LENGTH;

/**
 * The Mapper class for tokenizing a Japanese document.
 * @author kimura
 *
 */
public final class TokenizeMapper
    extends Mapper<Object, Text, Text, IntWritable> {

    /** the object for value of Mapper. */
    private static final IntWritable ONE = new IntWritable(1);

    /** the object for key of Mapper. */
    private Text word = new Text();

    /** Tokenizer instance. */
    private Tokenizer tokenizer;

    /** The constructor for TokenizeMapper class. */
    private TokenizeMapper() { }

    /**
     * The setup method for TokenizeMapper.
     *  This method will run before map phase.
     *  @param context Specify the hadoop Context object.
     */
    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String senConf = conf.get(PROP_SEN_CONF);
        tokenizer = new Tokenizer(senConf);
    }

    /**
     *  The map method for tokenize a Japanese document.
     * @param key Specify the map key.
     * @param value Specify the map value.
     * @param context Specify the hadoop Context object.
     * @throws IOException Exception for the input file.
     * @throws InterruptedException Exception for the waiting process.
     */
    @Override
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String maxLineLengthBuf = conf.get(MAX_LINE_LENGTH);
        int maxLineLength = Integer.valueOf(maxLineLengthBuf);
        String buf = value.toString();
        System.err.println(buf.length());
        if (buf.length() > maxLineLength) {
            buf = buf.substring(0, maxLineLength);
        }
        String[] result =
            tokenizer.getToken(buf, EnumSet.of(Tokenizer.ExtractType.Noun,
                    Tokenizer.ExtractType.Unk));
        for (String token : result) {
            word.set(token);
            context.write(word, ONE);
        }
    }
}
