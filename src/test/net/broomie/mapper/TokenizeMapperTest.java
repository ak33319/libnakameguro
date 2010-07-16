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
package test.net.broomie.mapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

import net.broomie.mapper.TokenizeMapper;
import net.broomie.reducer.TokenizeReducer;

import static org.easymock.classextension.EasyMock.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.junit.Before;

import org.junit.Test;
import org.junit.runner.JUnitCore;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import static net.broomie.ConstantsClass.LIB_NAKAMEGURO_CONF;

import static org.mockito.Mockito.*;


/**
 * TokenizeMapperTest class.
 * @author kimura
 *
 */
public class TokenizeMapperTest extends TestCase {


    /**
     * @param name Specify the class name.
     */
    public TokenizeMapperTest(String name) {
        super(name);
    }

    /** A tokenizer conf path. */
    private String tokenizerConf;

    /**
     * The set up method for Junit.
     * @throws Exception the Exception.
     */
    protected final void setUp() throws Exception {
        super.setUp();
        Properties prop = new Properties();
        prop.loadFromXML(new FileInputStream("conf/libnakameguro.xml"));
        tokenizerConf = prop.getProperty("libnakameguro.test.GoSen");

    }

    public void testTokenizerMapper() {
        TokenizeMapper mapper = new TokenizeMapper();

        Configuration conf = new Configuration();
        conf.set("libnakameguro.GoSen", tokenizerConf);
        //Context context = new Context(conf);
        //Context context = mock(Mapper.Context.class);
        //Context context = new Context(conf, null, null, null, null, null, null);
        Context context = mapper.super(conf, null, null, null, null, null, null);
        mapper.map(null, new Text("aiueo"), context);
        


    }

    /*
    @Test
    public void testTokenizeMapper() throws IOException, InterruptedException {
        TokenizeMapper mapper = new TokenizeMapper();
        Text value = new Text("aiueo");
        Context mock_context = mock(Context.class);
        Configuration conf = new Configuration();
        conf.addResource(LIB_NAKAMEGURO_CONF);
        mapper.map(null, new Text("hoge"), mock_context);
    }
    */

    /**
     * A test case for TokenizeMapper class.
     * @throws InterruptedException throw.
     * @throws IOException throw.
     */
    public final void testMap() throws IOException, InterruptedException {
        //TokenizeMapper mapper = new TokenizeMapper();
        //Configuration conf = new Configuration();
        //conf.addResource(LIB_NAKAMEGURO_CONF);


        //TokenizeMapper mapper = new TokenizeMapper();
        //TokenizeMapper.Context mock = createMock(TokenizeMapper.Context.class);
        //Text value = new Text("abc def");
        //mock.write(value, null);
        //mapper.map(null, value, mock);
        //mock.write(isA(Text.class), isA(IntWritable.class));
        //verify(mock);
    }
}
