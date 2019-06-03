package org.h2.index;

import java.util.ArrayList;

public class LinearHashBucket<K, V> {
    ArrayList<LinearHashPair<K, V>> rows;

    public LinearHashBucket() {
        rows = new ArrayList<>(10);
    }

    public LinearHashBucket(K key, V value) {
        rows = new ArrayList<>(10);
        rows.add(new LinearHashPair(key, value));
    }

    public void addRow(K key, V value) {
        rows.add(new LinearHashPair(key, value));
    }

//    public void removeRow(K key, V value) {
//        rows.remove(new LinearHashPair(key, value));
//    }

    public V getRow(K key) {
        for(LinearHashPair<K, V> row : rows) {
            if(key.equals(row.getFirst())) {
                return row.getSecond();
            }
        }
        return null;
    }

    public boolean isFull() {
        if(rows.size() >= 10)
            return true;
        return false;
    }
}
