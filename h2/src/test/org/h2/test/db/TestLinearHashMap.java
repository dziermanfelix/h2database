package org.h2.test.db;

import org.h2.index.LinearHashMap;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

import static org.junit.Assert.assertNotEquals;
import java.sql.SQLException;

public class TestLinearHashMap extends TestDb {
    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        testPutAndGet();
        testLargePutAndGet();
        testAllVariablesOnInsert();
        testRemove();
        testPutAndRemove();
        testRemoveNotExist();
    }

    private void testPutAndGet() {
        LinearHashMap linearHashMap = new LinearHashMap();
        linearHashMap.put(1, 1);
        assertEquals(1, linearHashMap.get(1));
    }

    private void testLargePutAndGet() {
        LinearHashMap linearHashMap = new LinearHashMap();
        int n = 5000;

        // fill it up
        for(long i = 0; i < n; i++) {
            linearHashMap.put(i, i);
        }

        // assert for each key value pair
        for(long i = 0; i < n; i++) {
            assertEquals(i, linearHashMap.get(i));
        }
    }

    private void testAllVariablesOnInsert() {
        LinearHashMap linearHashMap = new LinearHashMap();

        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(1, 1);
        assertEquals(1, linearHashMap.get(1));
        assertEquals(1, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(2, 2);
        assertEquals(2, linearHashMap.get(2));
        assertEquals(2, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(3, 3);
        assertEquals(3, linearHashMap.get(3));
        assertEquals(3, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(4, 4);
        assertEquals(4, linearHashMap.get(4));
        assertEquals(4, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(5, 5);
        assertEquals(5, linearHashMap.get(5));
        assertEquals(5, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(6, 6);
        assertEquals(6, linearHashMap.get(6));
        assertEquals(6, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(7, 7);
        assertEquals(7, linearHashMap.get(7));
        assertEquals(7, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(8, 8);
        assertEquals(8, linearHashMap.get(8));
        assertEquals(8, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(9, 9);
        assertEquals(9, linearHashMap.get(9));
        assertEquals(9, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(10, 10);
        assertEquals(10, linearHashMap.get(10));
        assertEquals(10, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(11, 11);
        assertEquals(11, linearHashMap.get(11));
        assertEquals(11, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(12, 12);
        assertEquals(12, linearHashMap.get(12));
        assertEquals(12, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(13, 13);
        assertEquals(13, linearHashMap.get(13));
        assertEquals(13, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(14, 14);
        assertEquals(14, linearHashMap.get(14));
        assertEquals(14, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(15, 15);
        assertEquals(15, linearHashMap.get(15));
        assertEquals(15, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(16, 16);
        assertEquals(16, linearHashMap.get(16));
        assertEquals(16, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(17, 17);
        assertEquals(17, linearHashMap.get(17));
        assertEquals(17, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(18, 18);
        assertEquals(18, linearHashMap.get(18));
        assertEquals(18, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(3, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(19, 19);
        assertEquals(19, linearHashMap.get(19));
        assertEquals(19, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(3, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        linearHashMap.put(20, 20);
        assertEquals(20, linearHashMap.get(20));
        assertEquals(20, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(3, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
    }

    private void testRemove() {
        LinearHashMap linearHashMap = new LinearHashMap();

        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        // put 1
        linearHashMap.put(1, 1);
        assertEquals(1, linearHashMap.get(1));
        assertEquals(1, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // remove 1
        linearHashMap.remove(1);
        assertEquals(null, linearHashMap.get(1));
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // should be empty again
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
    }

    private void testPutAndRemove() {
        LinearHashMap linearHashMap = new LinearHashMap();

        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        // put 1
        linearHashMap.put(1, 1);
        assertEquals(1, linearHashMap.get(1));
        assertEquals(1, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // remove 1
        linearHashMap.remove(1);
        assertEquals(null, linearHashMap.get(1));
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // should be empty again
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        // start over
        // put 1
        linearHashMap.put(1, 1);
        assertEquals(1, linearHashMap.get(1));
        assertEquals(1, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // put 2
        linearHashMap.put(2, 2);
        assertEquals(2, linearHashMap.get(2));
        assertEquals(2, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // put 3
        linearHashMap.put(3, 3);
        assertEquals(3, linearHashMap.get(3));
        assertEquals(3, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // put 4
        linearHashMap.put(4, 4);
        assertEquals(4, linearHashMap.get(4));
        assertEquals(4, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        // start removing
        // remove 1
        linearHashMap.remove(1);
        assertEquals(null, linearHashMap.get(1));
        assertEquals(3, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // remove 4
        linearHashMap.remove(4);
        assertEquals(null, linearHashMap.get(4));
        assertEquals(2, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // put 4
        linearHashMap.put(4, 4);
        assertEquals(4, linearHashMap.get(4));
        assertEquals(3, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // remove 3
        linearHashMap.remove(3);
        assertEquals(null, linearHashMap.get(3));
        assertEquals(2, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // remove 2
        linearHashMap.remove(2);
        assertEquals(null, linearHashMap.get(2));
        assertEquals(1, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // remove 1
        linearHashMap.remove(4);
        assertEquals(null, linearHashMap.get(4));
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        // should be empty again
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
    }

    private void testRemoveNotExist() {
        LinearHashMap linearHashMap = new LinearHashMap();

        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());

        try {
            // remove 1
            linearHashMap.remove(1);
            assertEquals(null, linearHashMap.get(1));
            assertEquals(0, linearHashMap.getR());
            assertEquals(2, linearHashMap.getI());
            assertEquals(2, linearHashMap.getN());
            assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
        } catch(NullPointerException e) {
            assertEquals(NullPointerException.class, e.getClass());
        }

        // should be empty again
        assertEquals(0, linearHashMap.getR());
        assertEquals(2, linearHashMap.getI());
        assertEquals(2, linearHashMap.getN());
        assertEquals(linearHashMap.getN(), linearHashMap.getBuckets().size());
    }
}
