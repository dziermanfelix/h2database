package org.h2.index;

import java.util.ArrayList;

public class LinearHashArray extends ArrayList<LinearHashBucket> {
    public LinearHashArray() {
        super();
    }

    public void addBucket(LinearHashBucket bucket) {
        this.add(bucket);
    }

    public void removeBucket(LinearHashBucket bucket) {
        this.remove(bucket);
    }

    public void printOut() {
        for(LinearHashBucket l : this) {
            System.out.println(l);
        }
    }
}
