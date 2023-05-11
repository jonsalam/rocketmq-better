package com.clouditora.mq.common.util;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BeanUtilTest {

    @Data
    static class Parent {
        private int a = 1;
        private String b = "b";
    }

    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @Data
    static class Child extends Parent {
        private String b = "3";
        private Integer c = 4;
    }

    @Test
    void copy() {
        Child source = new Child();
        Child target = BeanUtil.copy(source);
        assertNotSame(source, target);
        assertEquals(source, target);
        assertNotEquals(System.identityHashCode(source), System.identityHashCode(target));
    }
}
