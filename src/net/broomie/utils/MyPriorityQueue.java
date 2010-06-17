package net.broomie.utils;

import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Comparator;

/**
 *
 * @author kimura
 *
 */
public class MyPriorityQueue {

    /**
     *
     * @author kimura
     *
     */
    public class Entity {

        /**
         * aa.
         */
        private String key;

        /**
         *  aa.
         */
        private double val;

        /**
         *
         * @param key aa
         * @param val aa
         */
        public Entity(String key, double val) {
            this.key = key;
            this.val = val;
        }

        /**
         *
         * @return aaa
         */
        public final String getKey() {
            return key;
        }

        /**
         *
         * @return aa
         */
        public final double getVal() {
            return val;
        }
    }

    /**
     *
     * @author kimura
     *
     */
    public  class MyComparator implements Comparator<Entity> {
        @Override
        public final int compare(Entity ent1, Entity ent2) {

            if (ent1.getVal() > ent2.getVal()) {
                return 1;
            } else if (ent1.getVal() < ent2.getVal()) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    /**
     * aaa.
     */
    private Queue<Entity> queue;

    /**
     * aaa.
     */
    private int maxKeyNum;

    /**
     * comparator instance.
     */
    private MyComparator comparator;

    /**
     *
     * @param maxNum aaa
     */
    public MyPriorityQueue(int maxNum) {
        comparator = new MyComparator();
        queue = new PriorityQueue<Entity>(maxNum, comparator);
        maxKeyNum = maxNum;
    }

    /**
     *
     * @return queue size.
     */
    final int getSize() {
        return queue.size();
    }

    /**
     *
     * @param key input key.
     * @param val value of the key.
     */
    public final void add(String key, double val) {
        Entity newEnt = new Entity(key, val);
        Entity peekEnt = queue.peek();
        if (peekEnt == null) {
            queue.add(newEnt);
            return;
        }
        int result = comparator.compare(newEnt, peekEnt);
        if (result > 0) {
            if (queue.size() >= maxKeyNum) {
                queue.poll();
            }
            queue.add(newEnt);
        }
    }

    /**
     *
     * @return Entity
     */
    public final Entity poll() {
        if (queue.size() > 0) {
            return queue.poll();
        } else {
            return null;
        }
    }

    /**
     *
     * @param args command line.
     */
    public static void main(String[] args) {
        MyPriorityQueue queue = new MyPriorityQueue(5);

        queue.add("d", 4);
        queue.add("e", 5);
        queue.add("c", 3);
        queue.add("g", 7);
        queue.add("f", 6);
        queue.add("a", 1);
        queue.add("b", 2);
        queue.add("h", 8);
        queue.add("i", 10);

        Entity ent;
        while ((ent = queue.poll()) != null) {
            System.out.println(ent.getKey() + " " + ent.getVal());
        }
    }
}
