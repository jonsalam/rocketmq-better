package com.clouditora.mq.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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

    public static byte[] readBytes(InputStream input) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8 * 1024];
            int n;
            while ((n = input.read(buffer)) != -1) {
                output.write(buffer, 0, n);
            }
            return output.toByteArray();
        }
    }

    public static String file2String(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }
        byte[] bytes = readBytes(new FileInputStream(file));
        return new String(bytes);
    }

    public static String file2String(URL url) throws IOException {
        byte[] bytes = readBytes(url.openStream());
        return new String(bytes);
    }

    public static void overwriteFile(byte[] bytes, String filePath) throws IOException {
        // check if original file exists
        File originFile = new File(filePath);
        File parent = originFile.getParentFile();
        if (parent != null) {
            boolean mk = parent.mkdirs();
            log.debug("make dir {}: {}", parent, mk);
        }
        if (originFile.exists()) {
            // write to a temporary file
            File tmpFile = new File(filePath + ".tmp");
            try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                fos.write(bytes);
            }

            // rename original file to .bak with timestamp suffix
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
            File backFile = new File(String.format("%s.bak.%s", filePath, timestamp));
            if (backFile.exists()) {
                boolean deleted = backFile.delete();
                log.debug("delete back file {}: {}", backFile.getPath(), deleted);
            }
            boolean renamed = originFile.renameTo(backFile);
            log.debug("rename file {} to {}: {}", originFile.getPath(), backFile.getPath(), renamed);

            // rename temporary file to original file
            renamed = tmpFile.renameTo(originFile);
            log.debug("rename file {} to {}: {}", tmpFile.getPath(), originFile.getPath(), renamed);
        } else {
            try (FileOutputStream fos = new FileOutputStream(originFile)) {
                fos.write(bytes);
            }
        }
    }
}
