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

import java.io.IOException;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

import net.broomie.mapper.TokenizeMapper;

import static org.easymock.classextension.EasyMock.*;


/**
 * TokenizeMapperTest class.
 * @author kimura
 *
 */
public class TokenizeMapperTest extends TestCase {

    /**
     * A constructor for TokenizeMapperTest class.
     * @param name Specify the class name.
     */
    public TokenizeMapperTest(String name) {
        super(name);
    }

    /**
     * A test case for TokenizeMapper class.
     * @throws InterruptedException throw.
     * @throws IOException throw.
     */
    public final void testMap() throws IOException, InterruptedException {
        //TokenizeMapper mapper = new TokenizeMapper();
        //TokenizeMapper.Context mock = createMock(TokenizeMapper.Context.class);
        //Text value = new Text("abc def");
        //mock.write(value, null);
        //mapper.map(null, value, mock);
        //mock.write(isA(Text.class), isA(IntWritable.class));
        //verify(mock);
    }
}
