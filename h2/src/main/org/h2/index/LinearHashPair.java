package org.h2.index;

/**
 * This class represents a linear hash row in a linear hash bucket
 * @param <K> key
 * @param <V> value
 */
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
