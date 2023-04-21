package com.clouditora.mq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@AllArgsConstructor
public enum PermitBit {
    PERM_PRIORITY(0x1 << 3),
    PERM_READ(0x1 << 2),
    PERM_WRITE(0x1 << 1),
    PERM_INHERIT(0x1),
    ;

    private final int code;

    public static final int RW;
    public static final int SYSTEM;

    static {
        RW = PermitBit.mix(PermitBit.PERM_READ, PermitBit.PERM_WRITE);
        SYSTEM = PermitBit.mix(PermitBit.PERM_READ, PermitBit.PERM_WRITE, PERM_INHERIT);
    }

    public static int mix(PermitBit... bits) {
        if (ArrayUtils.isEmpty(bits)) {
            return 0;
        }
        int bit = 0;
        for (PermitBit permitBit : bits) {
            bit |= permitBit.code;
        }
        return bit;
    }

    @Override
    public String toString() {
        char isReadable = this == PERM_READ ? 'R' : '-';
        char isWriteable = this == PERM_READ ? 'W' : '-';
        char isInherited = this == PERM_READ ? 'X' : '-';
        return "%c%c%c".formatted(isReadable, isWriteable, isInherited);
    }
}
