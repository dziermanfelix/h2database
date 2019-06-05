package org.h2.index;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ExtensibleHashMap<K, V> implements Map<K, V> {
    //private K key;
    //private V value;
    // need prefix i .... 1->n reads left to right
    private int prefix;
    private ExtensibleHashBucket<K,V>[] table;
    private int size; // total elems

    @SuppressWarnings("unchecked")
    public ExtensibleHashMap() {
        //System.out.println("ExtensibleHashMap Default");
        //System.exit(1);
        this.prefix = 1;
        this.table = (ExtensibleHashBucket<K,V>[])(new Object[(int) Math.pow(2, prefix)]);
        this.size = 0;
    }

//    public ExtensibleHashMap(K key, V value) {
//        System.out.println("ExtensibleHashMap K-V");
//        System.exit(1);
//        this.key = key;
//        this.value = value;
//    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    } // need to implement buckets first

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        /*
        hash key
        check spot... if full -> need to increment i
        if space in bucket... add to bucket
        */
        //ExtensibleHashBucket<K, V> new_bucket = new ExtensibleHashBucket<K,V>(1);
        Pair<K,V> new_pair = new Pair<>(key, value);
        int front_bits_index = ExtensibleHashBucket.getFrontBits(this.prefix, key.hashCode());

        if(this.table[front_bits_index] == null) {
            ExtensibleHashBucket<K, V> new_bucket = new ExtensibleHashBucket<>(this.prefix);
            this.table[front_bits_index] = new_bucket;
        }

        ExtensibleHashBucket<K,V> curr_bucket = this.table[front_bits_index];

        if(!curr_bucket.isFull()) {
            curr_bucket.addValue(new_pair);
            this.size++;
        }
        else { // was full ... double or split
            if(this.prefix == curr_bucket.getPrefix()) {
                doubleTable();
            }
            else if(this.prefix > curr_bucket.getPrefix()) {
                //split table
                ArrayList<Pair<K,V>> new_list_zeros = curr_bucket.splitBucket(true);
                ArrayList<Pair<K,V>> new_list_ones = curr_bucket.splitBucket(false);
                curr_bucket.incrementPrefix();

                int new_front_bits = ExtensibleHashBucket.getFrontBits(curr_bucket.getPrefix(), key.hashCode());
                this.table[new_front_bits] = new ExtensibleHashBucket<>(curr_bucket.getPrefix());

                //doing both because cant tell which one will be overloaded
                //could do it so that its always the 0 staying put and 1 being put in new bucket..
                //worried that it might split from 001 to 000
                for(Pair<K,V> pair : new_list_zeros) {
                    put(pair.getKey(), pair.getValue());
                }
                for(Pair<K,V> pair : new_list_ones) {
                    put(pair.getKey(), pair.getValue());
                }
            }
            else {
                System.out.println("ERROR");
            }
            put(key, value);
        }

        return null; //doc says return previous key associate with value or null if that didnt exist
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map m) { //re mapping after full?

    }

    @Override
    public void clear() { //empty?

    }

    @Override
    public Set keySet() {
        return null;
    }

    @Override
    public Collection values() {
        return null;
    }

    @Override
    public Set<Map.Entry<K,V>> entrySet() {
        return null;
    }

//    public K getKey() {
//        return key;
//    }
//
//    public V getValue() {
//        return value;
//    }
    @SuppressWarnings("unchecked")
    private void doubleTable() {
        //double table
        int new_prefix = this.prefix + 1;
        ExtensibleHashBucket<K,V>[] new_table = (ExtensibleHashBucket<K,V>[])(new Object[(int) Math.pow(2, new_prefix)]);

        //point to old values
        for(int i = 0; i < this.table.length; i++) {
            int half_index = i / 2;
            new_table[i] = this.table[half_index];
        }

        this.table = new_table;
        this.prefix = new_prefix;
    }

    private void printTable() {
        for(int i = 0; i < this.table.length; i++) {
            System.out.println("Bucket " + i + ": ");
            if(this.table[i] == null) {
                System.out.println("nothing");
            }
            else {
                this.table[i].printAll();
            }
        }
    }



    public static void main(String[] args) {
        ExtensibleHashMap<Integer, Integer> map = new ExtensibleHashMap<>();
        map.printTable();
    }
}
