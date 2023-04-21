package com.clouditora.mq.common.util;

import lombok.extern.slf4j.Slf4j;

import java.beans.FeatureDescriptor;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class BeanUtil {
    private static final ConcurrentMap<Class<?>, List<PropertyDescriptor>> CACHE = new ConcurrentHashMap<>();

    public static <T> T copy(T source) {
        if (source == null) {
            return null;
        }
        try {
            T target = (T) source.getClass().getConstructor().newInstance();
            for (PropertyDescriptor pd : getPropertyDescriptors(source.getClass())) {
                Method getter = pd.getReadMethod();
                if (getter == null) {
                    continue;
                }
                Method setter = pd.getWriteMethod();
                if (setter == null) {
                    continue;
                }
                Object sourceValue = getter.invoke(source);
                setter.invoke(target, sourceValue);
            }
            return target;
        } catch (Exception e) {
            log.error("copy {} exception", source.getClass(), e);
            return null;
        }
    }

    private static List<PropertyDescriptor> getPropertyDescriptors(Class<?> clazz) {
        if (CACHE.containsKey(clazz)) {
            return CACHE.get(clazz);
        }
        return CACHE.computeIfAbsent(clazz, e -> getPropertyDescriptors(clazz, new ArrayList<>()));
    }

    private static List<PropertyDescriptor> getPropertyDescriptors(Class<?> clazz, List<PropertyDescriptor> list) {
        if (clazz == null || clazz.equals(Object.class)) {
            return list;
        }
        try {
            Set<String> names = list.stream().map(FeatureDescriptor::getName).collect(Collectors.toSet());
            List<PropertyDescriptor> descriptors = Arrays.stream(Introspector.getBeanInfo(clazz).getPropertyDescriptors())
                    .filter(e -> !names.contains(e.getName()))
                    .toList();
            list = Stream.of(descriptors, list).flatMap(Collection::stream).collect(Collectors.toList());
            return getPropertyDescriptors(clazz.getSuperclass(), list);
        } catch (Exception e) {
            log.error("get property descriptors exception: {}", clazz, e);
            return list;
        }
    }
}
