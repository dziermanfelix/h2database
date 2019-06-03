package org.h2.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LinearHashMap<K, V> implements Map<K, V> {
    private K key;
    private V value;
    private ArrayList<LinearHashBucket<K, V>> buckets;
    private Float capacity = 0.85f;

    /*
        n = num buckets
        r = num records
        i = num bits used to represent
     */
    private Integer i;
    private Integer r;
    private Integer n;

    public LinearHashMap() {
        super();
        r = 0;
        i = 2;
        n = 2;
        initializeBuckets();
    }

    /**
     * Initialize Buckets
     * Initializes the linear hash buckets.
     */
    private void initializeBuckets() {
        buckets = new ArrayList<>(n);
        for(int i = 0; i < n; i++) {
            LinearHashBucket bucket = new LinearHashBucket();
            buckets.add(bucket);
        }
    }

    @Override
    public int size() {
        System.out.println("LHM size()");
        return 0;
    }

    @Override
    public boolean isEmpty() {
        System.out.println("LHM isEmpty()");
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        System.out.println("LHM containsKey()");
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        System.out.println("LHM containsValue()");
        return false;
    }

//    /**
//     * Get Hash Bucket
//     * Gets the hash bucket based on the hash
//     * @param hash
//     * @return
//     */
//    private LinearHashBucket<K, V> getHashBucket(int hash) {
//        int bitmask = ~((~0) << this.i);
//
//        int hashedBlock = hash & bitmask;
//
//        // remove the leading bit if the block doesnt exist
//        if (hashedBlock > this.n) {
//            hashedBlock = hash & (bitmask >>> 1);
//        }
//
//        return buckets.get(hashedBlock);
//    }

    private int getHashedBlock(int hash) {
        int bitmask = ~((~0) << this.i);

        int hashedBlock = hash & bitmask;

        // remove the leading bit if the block doesnt exist
        if (hashedBlock > this.n) {
            hashedBlock = hash & (bitmask >>> 1);
        }

        return hashedBlock;
    }

    @Override
    public Object put(Object key, Object value) {
        System.out.println("i=" + i + ",n=" + n + ",r=" + r);

        // adding a record, increment r
        r++;
        float capCheck = (float) r / (n * 10);      // every bucket holds 10 rows

        if(capCheck >= capacity) {
            n++;
            buckets.add(new LinearHashBucket());

            int test = (int) (Math.pow(2, i) + 1);
            if(n == test) {
                i++;
            }
        }

        LinearHashBucket bucket = null;
        int m = getHashedBlock(key.hashCode());
        System.out.println("m=" + m);
        System.out.println("n=" + n);
        if(m < n) {
            bucket = buckets.get(m);
        }
        else if(m < ((int) (Math.pow(2, i)))) {
            // added the -1 at the end for out of bounds error ???
            int index = (int) Math.pow(2, (i - 1)) - 1;
            bucket = buckets.get(index);
        }
        bucket.addRow(key, value);

//        LinearHashBucket bucket = getHashBucket(key.hashCode());
//        bucket.addRow(key, value);

        // find an available bucket
//        boolean full = false;
//        for(LinearHashBucket b : buckets) {
//            if(!b.isFull()) {
//                b.addRow(key, value);
//                full = false;
//            }
//            else {
//                full = true;
//            }
//        }
        // no available buckets, add a new one
//        if(full) {
//            buckets.add(new LinearHashBucket(key, value));
//            n++;
//        }

        return null;
    }

    @Override
    public V get(Object key) {
        for(LinearHashBucket<K, V> bucket : buckets) {
            if(bucket.getValue((K) key) != null) {
                return bucket.getValue((K) key);
            }
        }
        return null;
    }

    @Override
    public V remove(Object key) {
        System.out.println("LHM remove()");
        r--;
        return null;
    }

    @Override
    public void putAll(Map m) {
        System.out.println("LHM putAll()");
    }

    @Override
    public void clear() {
        System.out.println("LHM clear()");
    }

    @Override
    public Set keySet() {
        System.out.println("LHM keySet()");
        return null;
    }

    @Override
    public Collection values() {
        System.out.println("LHM values()");
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}
