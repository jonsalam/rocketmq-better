package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.file.AbstractFileTest;
import com.clouditora.mq.store.file.GetMessageResult;
import com.clouditora.mq.store.serialize.ByteBufferDeserializer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.FutureTask;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageStoreTest extends AbstractFileTest {

    @Test
    public void getMessage() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setRootPath(path);
        MessageStore messageStore = new MessageStore(storeConfig);
        messageStore.startup();

        MessageEntity message = TestUtil.buildMessage();
        messageStore.asyncPut(message);

        FutureTask<GetMessageResult> task = new FutureTask<>(() -> {
            TestUtil.sleep(1);
            return messageStore.get(null, message.getTopic(), 0, 0, 16);
        });
        new Thread(task).start();

        GetMessageResult result = task.get();
        ByteBuffer wrap = ByteBuffer.wrap(result.covert2bytes());
        MessageEntity deserialize = new ByteBufferDeserializer().deserialize(wrap);
        assertEquals(message, deserialize);
    }
}
