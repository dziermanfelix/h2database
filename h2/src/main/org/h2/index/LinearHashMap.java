package org.h2.index;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LinearHashMap<K, V> implements Map {
    private K key;
    private V value;

    private Integer putCount = 0;
    private Integer getCount = 0;

    public LinearHashMap() {
//        System.out.println("LinearHashMap Default");
//        System.exit(1);
    }

    public LinearHashMap(K key, V value) {
        System.out.println("LinearHashMap K-V");
        System.exit(1);
        this.key = key;
        this.value = value;
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
    public Object get(Object key) {
        getCount++;
        System.out.println("LHM get()" + " " + getCount);
        return null;
    }

    @Override
    public Object put(Object key, Object value) {
        putCount++;
        System.out.println("LHM put()" + " " + putCount);
        return null;
    }

    @Override
    public Object remove(Object key) {
        System.out.println("LHM remove()");
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
    public Set<Entry> entrySet() {
        System.out.println("LHM entrySet()");
        return null;
    }

    public K getKey() {
        System.out.println("LHM getKey()");
        return key;
    }

    public V getValue() {
        System.out.println("LHM getValue()");
        return value;
    }
}
