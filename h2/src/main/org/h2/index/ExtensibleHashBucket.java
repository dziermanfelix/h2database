package org.h2.index;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;

public class ExtensibleHashBucket<K, V> {

    //private K key;
    private static int MAX_SIZE = 10;
    private int size;
    private int prefix; // amount of bits, starting from the left, to read
    //private E[] values;
    //private Pair<K, V>[] values;
    private ArrayList<Pair<K,V>> values;

    @SuppressWarnings("unchecked")
    public ExtensibleHashBucket(int prefix) {
        this.size = 0;
        this.prefix = prefix;
        this.values =  new ArrayList<>();

        //this.key = key;
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isFull() {
        return this.size == MAX_SIZE;
    }

//    public boolean isSameKey(K key) {
//        //might need a better equality check
//        return this.key.equals(key);
//    }

    public void incrementPrefix() {
        this.prefix++;
    }

    public int getPrefix() {
        return this.prefix;
    }

    public void setPrefix(int prefix) {
        this.prefix = prefix;
    }

    public boolean addValue(Pair value) {
        if(this.isFull()) {
            System.out.println("full");
            return false; // failed to add
        }
        //this.values.get(this.size) = value;
        this.values.add(value);
        this.size++;
        return true; //added successfully
    }

    public V getValue(K key) {
        for(int i = 0; i < this.size; i++) {
            if(this.values.get(i).getKey().equals(key)) {
                return this.values.get(i).getValue();
            }
        }
        return null;
    }

    public Pair<K, V> removeValue(K key) {
        int index = -1;
        Pair<K, V> ret_pair = null;
        for(int i = 0; i < this.size; i++) {
            if(this.values.get(i).getKey().equals(key)) {
                //return this.values[i].getValue();
                index = i;
                ret_pair = this.values.get(index);
                this.values.remove(index);
            }
        }
        this.size--;
        return ret_pair;
    }

    public ArrayList<Pair<K,V>> splitBucket(boolean isZero) { // nth significant bit
        //use to return list of pairs that no longer belong to the bucket
        int n = this.prefix + 1;
        ArrayList<Pair<K,V>> new_list = new ArrayList<>();

        int zero_one = isZero ? 0 : 1;

        Iterator<Pair<K,V>> iter = this.values.iterator();
        while(iter.hasNext()) {
            Pair<K,V> value = iter.next();
            if(getNthBit(n, value.getKey().hashCode()) == zero_one) {
                new_list.add(value);
                iter.remove();
            }
        }

        //this.incrementPrefix();
        return new_list;
    }

    public static int getFrontBits(int n, int number) {
        return (number >>> (32 - n));
    }

    public static int getNthBit(int n, int number) {
        return (number >>> (32 - n)) & 1;
    }

    public void printAll() {
        for(Pair<K,V> pair : this.values) {
            System.out.println("key: " + pair.getKey() + "value: " + pair.getValue());
        }
    }

    public static void main(String[] args) {
        System.out.println(ExtensibleHashBucket.getFrontBits(3, 2147483647));
        //this.getFrontBits(2, 4);
    }

}
