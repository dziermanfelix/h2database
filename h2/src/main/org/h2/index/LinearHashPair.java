package org.h2.index;

public class LinearHashPair<K, V> {
    public final K first;
    public final V second;

    public LinearHashPair(K first, V second) {
        this.first = first;
        this.second = second;
    }

    public K getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }
}
