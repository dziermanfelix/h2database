package org.h2.index;

import java.util.ArrayList;

/**
 * This class represents a hash bucket
 */
public class LinearHashBucket<K, V> {
    private ArrayList<LinearHashPair<K, V>> rows;
    private LinearHashBucket<K, V> overflowBucket;
    private boolean overflow = false;
    private int bucketCapacity = 10;

    public LinearHashBucket() {
        rows = new ArrayList<>(bucketCapacity);
    }

    public LinearHashBucket(K key, V value) {
        rows = new ArrayList<>(bucketCapacity);
        rows.add(new LinearHashPair(key, value));
    }

    /**
     * Add Row
     * Adds a row into the bucket.
     * @param key key to be stored
     * @param value value to be stored
     */
    public void addRow(K key, V value) {
        if(!this.overflow) {
            rows.add(new LinearHashPair(key, value));
        }
        else {
            if(overflow) {
                overflowBucket.addRow(key, value);
            }
            else {
                overflowBucket = new LinearHashBucket(key, value);
                overflow = true;
            }
        }
    }

    /**
     * Remove Row
     * Removes row from the bucket.
     * @param key Key to find correct row to remove.
     */
    public void removeRow(K key) {
        int index = -1;
        for(int i = 0; i < rows.size(); i++) {
            if(rows.get(i).getFirst() == key) {
                index = i;
            }
        }
        if(index != -1) {
            rows.remove(index);
        }
    }

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
        // check overflow bucket
        if(overflow) {
            return overflowBucket.getValue(key);
        }
        // key is not found
        return null;
    }

    public ArrayList<LinearHashPair<K, V>> getRows() {
        ArrayList<LinearHashPair<K, V>> listOfRows = new ArrayList<>(rows);
        if(overflow) {
            ArrayList<LinearHashPair<K, V>> overflowRows = new ArrayList<>(overflowBucket.getRows());
            for(LinearHashPair<K, V> pair : overflowRows) {
                listOfRows.add(pair);
            }
        }
        return listOfRows;
    }

    public boolean isOverflow() {
        return overflow;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        if(rows != null) {
            for (LinearHashPair<K, V> row : rows) {
                sb.append(row.getFirst().toString() + ",");
            }
        }
        sb.append("|");
        return sb.toString();
    }
}
