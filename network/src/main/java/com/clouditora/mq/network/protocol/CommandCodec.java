package com.clouditora.mq.network.protocol;

import com.clouditora.mq.common.constant.SerializeType;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.common.util.JsonUtil;
import com.clouditora.mq.network.exception.CommandException;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * ---- header ----
 * short:   code
 * byte:    language
 * short:   version
 * int:     opaque
 * int:     flag
 * String:  remark
 * HashMap: ext
 * ---- protocol ----
 * | length | serialize type & header length | header | body |
 * serialize type & header length: 占用同一个int, 第一个字节表示serialize type, 剩余的表示header length
 */
@Slf4j
public class CommandCodec {
    /**
     * @link org.apache.rocketmq.remoting.protocol.RemotingCommand#fastEncodeHeader
     */
    public static void encode(ByteBuf byteBuf, Command command) {
        encodeFields(byteBuf, command);
        encodeBody(byteBuf, command);
    }

    static void encodeFields(ByteBuf byteBuf, Command command) {
        int beginIndex = byteBuf.writerIndex();
        // skip, 因为一开始不知道长度
        byteBuf.writeLong(0);
        int fieldsLength;
        command.encodeHeader();
        if (command.getSerializeType() == SerializeType.BINARY) {
            int begin = byteBuf.writerIndex();
            encodeFieldsByBinary(byteBuf, command);
            int end = byteBuf.writerIndex();
            fieldsLength = end - begin;
        } else {
            byte[] bytes = encodeFieldsWithJson(command);
            byteBuf.writeBytes(bytes);
            fieldsLength = bytes.length;
        }
        // message length
        int bodyLength = getBodySize(command);
        byteBuf.setInt(beginIndex, fieldsLength + bodyLength + 4);
        // serialize type & fields length
        int fieldsFlag = encodeSerializeTypeAndFieldsLength(command.getSerializeType(), fieldsLength);
        byteBuf.setInt(beginIndex + 4, fieldsFlag);
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RocketMQSerializable#rocketMQProtocolEncode
     */
    static void encodeFieldsByBinary(ByteBuf byteBuf, Command command) {
        // code
        byteBuf.writeShort(command.getCode());
        // language
        byteBuf.writeByte(command.getLanguage().getCode());
        // version
        byteBuf.writeShort(command.getVersion());
        // opaque
        byteBuf.writeInt(command.getOpaque());
        // flag
        byteBuf.writeInt(command.getFlag());
        // remark
        String remark = command.getRemark();
        if (StringUtils.isEmpty(remark)) {
            byteBuf.writeInt(0);
        } else {
            encodeIntString(byteBuf, remark);
        }
        // ext
        encodeExtFields(byteBuf, command.getExtFields());
    }

    static int getBodySize(Command command) {
        byte[] body = command.getBody();
        return body != null ? body.length : 0;
    }

    static void encodeBody(ByteBuf byteBuf, Command command) {
        byte[] body = command.getBody();
        if (ArrayUtils.isEmpty(body)) {
            return;
        }
        byteBuf.writeBytes(body);
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RemotingCommand#decode
     */
    public static Command decode(ByteBuf byteBuf) throws CommandException {
        int messageLength = byteBuf.readableBytes();
        int fieldsFlag = byteBuf.readInt();
        int fieldsLength = decodeFieldsLength(fieldsFlag);
        if (fieldsLength > messageLength - 4) {
            throw new CommandException("decode error: bad fields length: " + fieldsLength);
        }

        SerializeType serializeType = decodeSerializeType(fieldsFlag);
        Command command;
        if (serializeType == SerializeType.BINARY) {
            command = decodeFieldsByBinary(byteBuf, fieldsLength, serializeType);
        } else {
            command = decodeFieldsByJson(byteBuf, fieldsLength);
        }

        int bodyLength = messageLength - 4 - fieldsLength;
        if (bodyLength > 0) {
            byte[] bodyData = new byte[bodyLength];
            byteBuf.readBytes(bodyData);
            command.setBody(bodyData);
        }
        return command;
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RocketMQSerializable#rocketMQProtocolDecode
     */
    static Command decodeFieldsByBinary(ByteBuf byteBuf, int fieldsLength, SerializeType serializeType) throws CommandException {
        Command command = new Command();
        // code
        command.setCode(byteBuf.readShort());
        // language
        command.setLanguage(EnumUtil.ofCode(byteBuf.readByte(), LanguageCode.class));
        // version
        command.setVersion(byteBuf.readShort());
        // opaque
        command.setOpaque(byteBuf.readInt());
        // flag
        command.setFlag(byteBuf.readInt());
        // remark
        command.setRemark(decodeIntString(byteBuf, fieldsLength));
        // ext
        command.setExtFields(decodeExtFields(byteBuf, fieldsLength));
        command.setSerializeType(serializeType);
        return command;
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RocketMQSerializable#writeStr
     */
    static void encodeShortString(ByteBuf byteBuf, String string) {
        int index = byteBuf.writerIndex();
        // skip, 因为一开始不知道长度
        byteBuf.writeShort(0);
        int length = byteBuf.writeCharSequence(string, StandardCharsets.UTF_8);
        byteBuf.setShort(index, length);
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RocketMQSerializable#writeStr
     */
    static void encodeIntString(ByteBuf byteBuf, String string) {
        int index = byteBuf.writerIndex();
        // skip, 因为一开始不知道长度
        byteBuf.writeInt(0);
        int length = byteBuf.writeCharSequence(string, StandardCharsets.UTF_8);
        byteBuf.setInt(index, length);
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RocketMQSerializable#readStr
     */
    static String decodeShortString(ByteBuf byteBuf, int limit) throws CommandException {
        short length = byteBuf.readShort();
        return decodeString(byteBuf, length, limit);
    }

    /**
     * @link org.apache.rocketmq.remoting.protocol.RocketMQSerializable#readStr
     */
    static String decodeIntString(ByteBuf byteBuf, int limit) throws CommandException {
        int length = byteBuf.readInt();
        return decodeString(byteBuf, length, limit);
    }

    static String decodeString(ByteBuf byteBuf, int length, int limit) throws CommandException {
        if (length == 0) {
            return null;
        }
        if (length > limit) {
            throw new CommandException(String.format("decode error: string over length: %s/%s", length, limit));
        }
        return byteBuf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    }

    static int encodeSerializeTypeAndFieldsLength(SerializeType type, int fieldsLength) {
        return (type.getCode() << 24) | (fieldsLength & 0x00FFFFFF);
    }

    static int decodeFieldsLength(int flag) {
        return flag & 0x00FFFFFF;
    }

    static SerializeType decodeSerializeType(int flag) {
        byte code = (byte) ((flag >> 24) & 0xFF);
        return EnumUtil.ofCode(code, SerializeType.class);
    }

    /**
     * @link
     */
    static void encodeExtFields(ByteBuf byteBuf, Map<String, String> extFields) {
        int startIndex = byteBuf.writerIndex();
        // skip, 因为一开始不知道长度
        byteBuf.writeInt(0);
        if (MapUtils.isNotEmpty(extFields)) {
            extFields.forEach((k, v) -> {
                if (k != null && v != null) {
                    encodeShortString(byteBuf, k);
                    encodeIntString(byteBuf, v);
                }
            });
        }
        byteBuf.setInt(startIndex, byteBuf.writerIndex() - startIndex - 4);
    }

    static Map<String, String> decodeExtFields(ByteBuf byteBuf, int limit) throws CommandException {
        int extFieldsLength = byteBuf.readInt();
        if (extFieldsLength > 0 && extFieldsLength > limit) {
            throw new CommandException(String.format("decode error: ext fields over length: %s/%s", extFieldsLength, limit));
        }
        Map<String, String> map = new HashMap<>();
        int endIndex = byteBuf.readerIndex() + extFieldsLength;
        while (byteBuf.readerIndex() < endIndex) {
            String k = decodeShortString(byteBuf, extFieldsLength);
            String v = decodeIntString(byteBuf, extFieldsLength);
            map.put(k, v);
        }
        return map;
    }

    static Command decodeFieldsByJson(ByteBuf byteBuf, int length) {
        byte[] bytes = new byte[length];
        byteBuf.readBytes(bytes);
        return JsonUtil.parse(bytes, Command.class);
    }

    static byte[] encodeFieldsWithJson(Command command) {
        return JsonUtil.toBytes(command);
    }
}
