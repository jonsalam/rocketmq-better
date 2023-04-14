package com.clouditora.mq.common.util;

import java.io.*;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FileUtil {
    public static byte[] readBytes(InputStream input) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int n;
            while ((n = input.read(buffer)) != -1) {
                output.write(buffer, 0, n);
            }
            return output.toByteArray();
        }
    }

    public static String file2String(String fileName) throws IOException {
        byte[] bytes = readBytes(new FileInputStream(fileName));
        return new String(bytes);
    }

    public static String file2String(URL url) throws IOException {
        byte[] bytes = readBytes(url.openStream());
        return new String(bytes);
    }

    public static void overwriteFile(byte[] bytes, String filePath) throws IOException {
        // check if original file exists
        File origFile = new File(filePath);
        if (origFile.exists()) {
            // write to a temporary file
            File tmpFile = new File(filePath + ".tmp");
            try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                fos.write(bytes);
            }

            // rename original file to .bak with timestamp suffix
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
            File bakFile = new File(String.format("%s.bak.%s", filePath, timestamp));
            if (bakFile.exists()) {
                bakFile.delete();
            }
            origFile.renameTo(bakFile);

            // rename temporary file to original file
            tmpFile.renameTo(origFile);
        } else {
            try (FileOutputStream fos = new FileOutputStream(origFile)) {
                fos.write(bytes);
            }
        }
    }
}
