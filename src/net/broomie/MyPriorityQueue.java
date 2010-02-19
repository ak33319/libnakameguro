package net.broomie;

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
        private int val;

        /**
         *
         * @param key aa
         * @param val aa
         */
        public Entity(String key, int val) {
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
        public final int getVal() {
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
     *
     * @param maxNum aaa
     */
    MyPriorityQueue(int maxNum) {
        queue = new PriorityQueue<Entity>(maxNum, new MyComparator());
        maxKeyNum = maxNum;
    }

    /**
     *
     * @param key aaa
     * @param val aaa
     */
    public final void add(String key, int val) {
        Entity ent = new Entity(key, val);
        queue.add(ent);
        if (queue.size() > maxKeyNum) {
            queue.poll();
        }
    }

    /**
     * aaa.
     */
    public final void showQueue() {
        while (true) {
            Entity ent = queue.poll();
            if (ent == null) {
                System.out.println("break");
                break;
            }
            System.out.println(ent.getKey() + " " + ent.getVal());
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

        queue.showQueue();

    }

}
