package com.clouditora.mq.store.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.text.NumberFormat;

@Slf4j
public class FileUtil {

    public static void mkdir(String path) {
        if (path == null) {
            return;
        }
        File f = new File(path);
        if (f.exists()) {
            return;
        }
        boolean result = f.mkdirs();
        log.info("mkdir {} {}", path, result ? "OK" : "Failed");
    }

    public static long string2Long(String str) {
        return Long.parseLong(str);
    }

    public static String long2String(long l) {
        NumberFormat format = NumberFormat.getInstance();
        format.setMinimumIntegerDigits(20);
        format.setMaximumFractionDigits(0);
        format.setGroupingUsed(false);
        return format.format(l);
    }
}
