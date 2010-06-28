
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

package test.net.broomie.utils;

import java.util.Arrays;
import java.util.Comparator;

import junit.framework.TestCase;
import net.broomie.utils.MyPriorityQueue;

/**
 * Test case for MyPriorityQueue class.
 * @author kimura
 */
public class MyPriorityQueueTest extends TestCase {

    /**
     * A class for testing each value of queue.
     * @author kimura
     */
    public class TestEntity {

        /** The buffer for storing key of entity. */
        private String key;

        /** The buffer for storing value of entity. */
        private double val;

        /**
         * The constructor for TestEntity class.
         * @param key Specify the key for entity.
         * @param val Specify the value for entity.
         */
        public TestEntity(String key, double val) {
            this.key = key;
            this.val = val;
        }

        /**
         * A accsessor for key of the entity.
         * @return Return the key of the entity.
         */
        public final String getKey() {
            return key;
        }

        /**
         * A accessor for value of the entity.
         * @return Return the value of the entity.
         */
        public final double getVal() {
            return val;
        }
    }

    /**
     * A Comaparator implements for test.
     * @author kimura
     */
    public class TestComparator implements Comparator<Object> {

        @Override
        public final int compare(Object o1, Object o2) {
            TestEntity ent1 = (TestEntity) o1;
            TestEntity ent2 = (TestEntity) o2;

            if (ent1.getVal() > ent2.getVal()) {
                return -1;
            } else if (ent1.getVal() < ent2.getVal()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /** A Default queue size .*/
    private final int defaultQueueSize = 10;

    /** A Number of test line. */
    private final int testNum = 20;

    /** A queue class. */
    private MyPriorityQueue queue;

    /**
     * setup method.
     * @throws Exception throw Exception.
      */
    protected final void setUp() throws Exception {
        super.setUp();
        queue = new MyPriorityQueue(defaultQueueSize);
    }

    /**
     * The tear down method for Junit.
     * @throws Exception throw Exception.
     */
    protected final void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * A test for MyPriorityQueue class.
     */
    public final void testMyPriorityQueue() {
        TestEntity[] entities = new TestEntity[testNum];
        for (int i = 0; i < testNum; i++) {
            String key =
                org.apache.commons.lang.RandomStringUtils.randomAscii(10);
            double val = java.lang.Math.random();
            TestEntity ent = new TestEntity(key, val);
            entities[i] = ent;
            System.err.println("[MyPriorityQueue] input:" + key + "\t" + val);
            queue.add(key, val);
        }
        System.err.println("[MyPriorityQueue] queue size:" + queue.getSize());
        TestComparator comp = new TestComparator();
        //Arrays.sort(entities, new TestComparator());
        Arrays.sort(entities, comp);
        int cnt = 0;
        MyPriorityQueue.Entity ent;
        int queueSiz = queue.getSize();
        TestEntity[] queueEntities = new TestEntity[queueSiz];
        while ((ent = queue.poll()) != null) {
            TestEntity entBuf = new TestEntity(ent.getKey(), ent.getVal());
            queueEntities[queue.getSize()] = entBuf;
            cnt++;
        }
        for (int i  = 0; i < queueSiz; i++) {
            TestEntity queueEntityBuf = queueEntities[i];
            TestEntity checkEntityBuf = entities[i];
            System.err.println("[MyPriorityQueue] output:"
                    + queueEntityBuf.getKey()
                    + "\t" + queueEntityBuf.getVal());
            assertTrue(queueEntityBuf.getKey().equals(checkEntityBuf.key));
            assertEquals(queueEntityBuf.getVal(), checkEntityBuf.getVal());
        }

    }
}
