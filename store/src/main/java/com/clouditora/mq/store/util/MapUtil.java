package com.clouditora.mq.store.util;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class MapUtil {
    public static <K, V> V putIfAbsentAtomically(ConcurrentMap<K, V> map, K key, Function<K, V> mapping) {
        V value = map.get(key);
        if (value == null) {
            V newValue = mapping.apply(key);
            V oldValue = map.putIfAbsent(key, newValue);
            if (oldValue != null) {
                value = oldValue;
            } else {
                value = newValue;
            }
        }
        return value;
    }
}
