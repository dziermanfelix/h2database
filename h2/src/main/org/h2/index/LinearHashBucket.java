package org.h2.index;

import java.util.ArrayList;

/**
 * This class represents a hash bucket
 */
public class LinearHashBucket<K, V> {
    ArrayList<LinearHashPair<K, V>> rows;
    LinearHashBucket overflowBucket;

    public LinearHashBucket() {
        rows = new ArrayList<>(10);
    }

    public LinearHashBucket(K key, V value) {
        rows = new ArrayList<>(10);
        rows.add(new LinearHashPair(key, value));
    }

    /**
     * Add Row
     * Adds a row into the bucket.
     * @param key key to be stored
     * @param value value to be stored
     */
    public void addRow(K key, V value) {
        if(!this.isFull()) {
            rows.add(new LinearHashPair(key, value));
        }
        else {
            overflowBucket = new LinearHashBucket(key, value);
        }
    }

//    public void removeRow(K key, V value) {
//        rows.remove(new LinearHashPair(key, value));
//    }

    /**
     * Get Value
     * Gets the value associated with the key
     * @param key The search key
     * @return The value
     */
    public V getValue(K key) {
        // check this bucket
        for(LinearHashPair<K, V> row : rows) {
            if(key.equals(row.getFirst())) {
                return row.getSecond();
            }
        }

        // check overflow bucket before returning null
        if(this.isFull()) {
            for(LinearHashPair<K, V> row : rows) {
                if(key.equals(row.getFirst())) {
                    return row.getSecond();
                }
            }
        }

        // key is not found
        return null;
    }

    /**
     * Is Full
     * Checks to see if this bucket is full
     * @return True if size is 10
     */
    public boolean isFull() {
        if(rows.size() >= 10)
            return true;
        return false;
    }
}
