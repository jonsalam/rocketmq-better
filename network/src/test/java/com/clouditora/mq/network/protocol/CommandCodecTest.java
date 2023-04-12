package com.clouditora.mq.network.protocol;

import com.clouditora.mq.network.RequestHeader;
import com.clouditora.mq.network.exception.CommandException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandCodecTest {

    @Test
    void codecShortString() throws CommandException {
        String encode = "hello world";
        ByteBuf buffer = Unpooled.buffer();

        CommandCodec.encodeShortString(buffer, encode);
        String decode = CommandCodec.decodeShortString(buffer, buffer.writerIndex());
        assertEquals(encode, decode);
    }

    @Test
    void codecIntString() throws CommandException {
        String encode = "hello world";
        ByteBuf buffer = Unpooled.buffer();

        CommandCodec.encodeIntString(buffer, encode);
        String decode = CommandCodec.decodeIntString(buffer, buffer.writerIndex());
        assertEquals(encode, decode);
    }

    @Test
    void codecExtFields() throws CommandException {
        Map<String, String> extFields = new HashMap<>();
        extFields.put("hello", "world");

        ByteBuf buffer = Unpooled.buffer();
        CommandCodec.encodeExtFields(buffer, extFields);

        Map<String, String> decodeExtFields = CommandCodec.decodeExtFields(buffer, buffer.writerIndex());
        assertEquals(extFields, decodeExtFields);
    }

    @Test
    void codecFieldWithJson() {
        RequestHeader header = new RequestHeader();
        header.setCount(1);
        header.setMessageTitle("message");

        Command encode = new Command();
        encode.setCode(1);
        encode.setVersion(2);
        encode.setOpaque(3);
        encode.setFlag(4);
        encode.setRemark("remark");
        encode.setExtFields(Map.of("hello", "world"));
        encode.setHeader(header);
        encode.headerToExtFields();
        encode.setHeader(null);
        encode.setSerializeType(SerializeType.JSON);

        byte[] bytes = CommandCodec.encodeFieldsWithJson(encode);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        Command decode = CommandCodec.decodeFieldsByJson(byteBuf, bytes.length);

        assertEquals(encode, decode);
    }
}
