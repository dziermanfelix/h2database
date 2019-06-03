package org.h2.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LinearHashMap<K, V> implements Map<K, V> {
    private K key;
    private V value;
    ArrayList<LinearHashBucket<K, V>> buckets;

    /*
        n = num buckets
        r = num records
        i = num bits used to represent
     */
    private Integer i = 0;
    private Integer r = 0;
    private Integer n = 0;

    public LinearHashMap() {
        super();
        LinearHashBucket bucket = new LinearHashBucket();
        buckets = new ArrayList<>(1);
        buckets.add(bucket);
        n = 1;
        i = 2;
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

    @Override
    public V get(Object key) {
        for(LinearHashBucket<K, V> bucket : buckets) {
            if(bucket.getRow((K) key) != null) {
                return bucket.getRow((K) key);
            }
        }
        return null;
    }

    @Override
    public Object put(Object key, Object value) {
//        System.out.println("i=" + i + ",n=" + n + ",r=" + r);
        // adding a record, increment r
        r++;
        // find an available bucket
        boolean full = false;
        for(LinearHashBucket b : buckets) {
            if(!b.isFull()) {
                b.addRow(key, value);
                full = false;
            }
            else {
                full = true;
            }
        }
        // no available buckets, add a new one
        if(full) {
            buckets.add(new LinearHashBucket(key, value));
            n++;
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
