package com.clouditora.mq.store.index;

import com.clouditora.mq.store.AbstractFileTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class IndexFileTest extends AbstractFileTest {

    @Test
    public void putKey() throws IOException {
        path += "index/100";
        IndexFile indexFile = new IndexFile(path, 100, 5);
//        for (int i = 0; i < 5; i++) {
//            indexFile.putKey("0", 0, System.currentTimeMillis());
//        }
        indexFile.putKey("0", 0, System.currentTimeMillis());
        indexFile.putKey("a", 1, System.currentTimeMillis());
        indexFile.putKey("0", 2, System.currentTimeMillis());
    }
}
