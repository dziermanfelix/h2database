package org.h2.index;

public class CustomHashMapTest {

    public static void main(String[] args) {
        String key = "DC";
        Integer value = 4;
        LinearHashMap<String, Integer> map = new LinearHashMap<>(key, value);
        System.out.println(map.getKey() + "->" + map.getValue());

        ExtensibleHashMap<String, Integer> extMap = new ExtensibleHashMap<>(key, value);
        System.out.println(extMap.getKey() + "->" + extMap.getValue());
    }
}
