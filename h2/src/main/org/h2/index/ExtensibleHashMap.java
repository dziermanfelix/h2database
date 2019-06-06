package org.h2.index;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.AbstractMap.SimpleEntry;

public class ExtensibleHashMap<K, V> implements Map<K, V> {
    //private K key;
    //private V value;
    // need prefix i .... 1->n reads left to right
    private int prefix;
    private int length;
    //private ExtensibleHashBucket<K,V>[] table;
    private ArrayList<ExtensibleHashBucket<K,V>> table;

    private int size; // total elems

    public ExtensibleHashMap() {
        //System.out.println("ExtensibleHashMap Default");
        //System.exit(1);
        this.prefix = 1;
        this.length = (int) Math.pow(2, prefix);
        //this.table = (ExtensibleHashBucket<K,V>[])(new Object[(int) Math.pow(2, prefix)]);
        //this.table = new Object[(int) Math.pow(2, prefix)];
        this.table = new ArrayList<>();
        while(this.table.size() < this.length) {
            this.table.add(null);
        }
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

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object key) {
        //boolean has_Key = false;

        ExtensibleHashBucket<K,V> bucket = this.table.get(ExtensibleHashBucket.getFrontBits(this.prefix, key.hashCode()));
        boolean has_Key = bucket.hasKey((K)key);
        if(has_Key) {
            return has_Key;
        }
        return has_Key;
    } // need to implement buckets first

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsValue(Object value) {

        for(ExtensibleHashBucket<K,V> bucket : this.table) {
            if(bucket.hasValue((V)value)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        if(key == null) {
            return null;
        }

        int front_bits_index = ExtensibleHashBucket.getFrontBits(this.prefix, key.hashCode());

        if(this.table.get(front_bits_index) == null) {
            return null;
        }
        ExtensibleHashBucket<K,V> curr_bucket = this.table.get(front_bits_index);
        //is this ok????
        return curr_bucket.getValue((K)key);
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
        //System.out.println("prefix: "  + this.prefix);
        System.out.println("put key: "  + key);
        //System.out.println("put val: "  + value);

        Pair<K,V> new_pair = new Pair<>(key, value);
        int front_bits_index = ExtensibleHashBucket.getFrontBits(this.prefix, key.hashCode());

        if(this.table.get(front_bits_index) == null) {
            ExtensibleHashBucket<K, V> new_bucket = new ExtensibleHashBucket<>(this.prefix);
            this.table.set(front_bits_index, new_bucket);
        }

        ExtensibleHashBucket<K,V> curr_bucket = this.table.get(front_bits_index);

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
                //System.out.println("Creating new bucket...");
                this.table.set(new_front_bits, new ExtensibleHashBucket<K,V>(curr_bucket.getPrefix()));

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
    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        if(key == null) {
            return null;
        }

        for(ExtensibleHashBucket<K,V> bucket : this.table) {
            V val = bucket.removeValue((K)key);
            if(val != null) {
                this.size--;
                return val;
            }
        }
        return null;
    }

    @Override
    public void putAll(Map m) { //re mapping after full?

    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() { //empty?
        this.table = new ArrayList<>((int) Math.pow(2, prefix));
    }

    @Override
    public Set keySet() {
        Set<K> set = new HashSet<>();

        for(ExtensibleHashBucket<K,V> bucket : this.table) {
            bucket.placeKeyInSet(set);
        }

        return set;
    }

    @Override
    public Collection values() {
        return null;
    }

    @Override
    public Set<Map.Entry<K,V>> entrySet() {
        Set<Map.Entry<K,V>> set = new HashSet<>();

        //Set<SimpleEntry<K,V>> set = new HashSet<>();

        for(ExtensibleHashBucket<K,V> bucket : this.table) {
            bucket.placePairsInSet(set);
        }

        //Set<Map.Entry<K,V>> ret = (Set<Map.Entry<K,V>>)set;
        return set;
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
        //ExtensibleHashBucket<K,V>[] new_table = (ExtensibleHashBucket<K,V>[])(new Object[(int) Math.pow(2, new_prefix)]);
        ArrayList<ExtensibleHashBucket<K,V>> new_table = new ArrayList<>();
        //point to old values
//        for(int i = 0; i < this.table.size(); i++) {
//            int half_index = i / 2;
//            new_table.set(i, this.table.get(half_index));
//        }

        System.out.println("doubling");
        System.out.println("old length: " + this.table.size());

        for(int i = 0; i < this.table.size(); i++) {
            ExtensibleHashBucket<K,V> bucket = this.table.get(i);
            new_table.add(bucket);
            new_table.add(bucket);
        }
        System.out.println("new length: " + new_table.size());
        this.table = new_table;
        this.length = this.table.size();
        //this.table = new_table;
        this.prefix = new_prefix;
    }

    private void printTable() {
//        for(int i = 0; i < this.table.length; i++) {
//            System.out.println("Bucket " + i + ": ");
//            if(this.table[i] == null) {
//                System.out.println("nothing");
//            }
//            else {
//                this.table[i].printAll();
//            }
//        }
        int i = 0;
        System.out.println(this.table.size());
        for(ExtensibleHashBucket<K,V> bucket : this.table) {
            System.out.println("Bucket " + i + ": ");
            i++;
            if (bucket == null) {
                System.out.println("nothing");
            }
            else {
                System.out.println("prefix: " + bucket.getPrefix());
                bucket.printAll();
            }

        }
    }



    public static void main(String[] args) {
        System.out.println("start");
        ExtensibleHashMap<Integer, Integer> map = new ExtensibleHashMap<>();
        //test 1 - good just putting regular to fill
//        map.put(0x00000000, 0x00000000);
//        map.put(0x10000000, 0x10000000);
//
//        map.put(0x80000000, 0x80000000);
//        map.put(0x90000000, 0x90000000);
//
//        System.out.println("my value: " + 0xC0000000);
//        map.put(0x40000000, 0x40000000);
//        map.put(0xC0000000, 0xC0000000);
        //test 2 - good double and put
//        map.put(0x00000000, 0x00000000);
//        map.put(0x20000000, 0x00000000);
//        map.put(0x40000000, 0x00000000);
//        map.put(0x60000000, 0x00000000);
//        map.put(0x10000000, 0x00000000);
        //test 3 - good double and put
//        map.put(0x80000000, 0x00000000);
//        map.put(0xA0000000, 0x00000000);
//        map.put(0xC0000000, 0x00000000);
//        map.put(0xE0000000, 0x00000000);
//        map.put(0xF0000000, 0x00000000);

        //test 4 - good gets
//        map.put(0x00000000, 0x00000001);
//        map.put(0x20000000, 0x00000002);
//        map.put(0x40000000, 0x00000003);
//        map.put(0x60000000, 0x00000004);
//        map.put(0x10000000, 0x00000005);
//        map.put(0x80000000, 0x00000006);
//        map.put(0xA0000000, 0x00000007);
//        map.put(0xC0000000, 0x00000008);
//        map.put(0xE0000000, 0x00000009);
//        map.put(0xF0000000, 0x0000000A);
//
//        map.printTable();
//
//        System.out.println(map.get(0x00000000).equals(0x00000001));
//        System.out.println(map.get(0x10000000).equals(0x00000005));
//        System.out.println(map.get(0xC0000000).equals(0x00000008));

        System.out.println("end");
    }
}
