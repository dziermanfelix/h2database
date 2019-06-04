package org.h2.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LinearHashMap<K, V> implements Map<K, V> {
    private ArrayList<LinearHashBucket<K, V>> buckets;
    private Float maxCapacityPercentage = 0.85f;
    private int i;     // num buckets
    private int r;     // num records
    private int n;     // num bits to use from hash

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

    /**
     * Get Hash
     * Get hash code based on key.
     * Last i bits of hash code.
     * @param hashCode Hash code to get last i bits of.
     * @return Last i bits of hash code.
     */
    private int getHash(int hashCode) {
        int bitmask = ~((~0) << this.i);
        int hash = hashCode & bitmask;
        // remove the leading bit if the block doesnt exist
        if (hash > this.n) {
            hash = hash & (bitmask >>> 1);
        }
        return hash;
    }

    /**
     * Get Bucket Using Linear Hashing
     * Uses the linear hashing algorithm to get the bucket that the key belongs to.
     * @param key Key to base hash off.
     * @return Bucket the key belongs to.
     */
    private LinearHashBucket<K, V> getBucketUsingLinearHashing(Object key) {
        LinearHashBucket<K, V> bucket = null;
        int m = getHash(key.hashCode());
        if(m < n) {
            bucket = buckets.get(m);
        }
        else if(m < ((int) (Math.pow(2, i)))) {
            int index = m - (int) (Math.pow(2, (i - 1)));
            bucket = buckets.get(index);
        }
        return bucket;
    }

    /**
     * Remove Old Rows
     * When splitting a bucket, we must remove the old rows that were moved to a new bucket.
     * @param bucket Bucket to remove rows from.
     * @param list List of rows to remove.
     */
    private void removeOldRows(LinearHashBucket<K, V> bucket, ArrayList<LinearHashPair<K, V>> list) {
        for(LinearHashPair<K, V> row : list) {
            bucket.removeRow(row.getFirst());
        }
    }

    /**
     * Split Bucket
     * Part of the linear hashing algorithm.
     * @param n Old n value.
     */
    private void splitBucket(int n) {
        int index = (n - (int) Math.pow(2, (i - 1)));
        LinearHashBucket<K, V> currentBucket = buckets.get(index);
        ArrayList<LinearHashPair<K, V>> rowsToRemove = new ArrayList<>();
        for(LinearHashPair<K, V> pair : currentBucket.getRows()) {
            K key = pair.getFirst();
            V value = pair.getSecond();
            LinearHashBucket<K, V> splitBucket = getBucketUsingLinearHashing(key);
            if(!currentBucket.equals(splitBucket)) {
                splitBucket.addRow(key, value);
                rowsToRemove.add(pair);
            }
        }
        removeOldRows(currentBucket, rowsToRemove);
    }

    /**
     * Check Capacity And Variables
     * Part of the linear hashing algorithm.
     * Manages the variables.
     */
    private void checkCapacityAndVariables() {
        float capCheck = (float) r / (n * 10);      // every bucket holds 10 rows
        if(capCheck >= maxCapacityPercentage) {
            n++;
            LinearHashBucket<K, V> bucketToAdd = new LinearHashBucket<>();
            buckets.add(bucketToAdd);

            int checkIncrementi = (int) (Math.pow(2, i) + 1);
            if(n == checkIncrementi) {
                i++;
            }

            // using old n...
            splitBucket(n - 1);
        }
    }

    @Override
    public Object put(Object key, Object value) {
        checkCapacityAndVariables();
        LinearHashBucket<K, V> bucket = getBucketUsingLinearHashing(key);
        bucket.addRow((K)key, (V)value);
        r++;
        return null;
    }

    @Override
    public V get(Object key) {
        LinearHashBucket<K, V> bucket = getBucketUsingLinearHashing(key);
        return bucket.getValue((K)key);
    }

    // not actually using hash
    @Override
    public V remove(Object key) {
        LinearHashBucket<K, V> bucket = getBucketUsingLinearHashing(key);
        V value = bucket.getValue((K) key);
        if(value != null) {
            bucket.removeRow((K) key);
            r--;
        }
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

    public int getI() {
        return i;
    }

    public int getR() {
        return r;
    }

    public int getN() {
        return n;
    }

    public ArrayList<LinearHashBucket<K, V>> getBuckets() {
        return buckets;
    }
}
