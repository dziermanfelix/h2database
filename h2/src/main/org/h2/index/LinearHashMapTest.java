package org.h2.index;

public class LinearHashMapTest {

    public static void main(String[] args) {
        LinearHashMap<String, String> map = new LinearHashMap<>("borat", "clemson");
        System.out.println(map.getKey() + "->" + map.getValue());
    }
}
