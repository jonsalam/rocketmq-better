package com.clouditora.mq.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;

@Slf4j
public class NetworkUtil {

    /**
     * 不考虑ipv6
     *
     * @link org.apache.rocketmq.remoting.common.RemotingUtil#getLocalAddress
     */
    public static String getLocalIp() {
        try {
            return NetworkInterface.networkInterfaces()
                    .filter(e -> !isBridge(e))
                    .flatMap(NetworkInterface::inetAddresses)
                    .filter(e -> !e.isLoopbackAddress())
                    .filter(e -> e instanceof Inet4Address)
                    .map(InetAddress::getHostAddress)
                    .filter(e -> !e.startsWith("127.0"))
                    .filter(e -> !e.startsWith("192.168"))
                    .findFirst()
                    .orElse(InetAddress.getLocalHost().getHostAddress());
        } catch (Exception e) {
            log.error("Failed to obtain local ip", e);
        }
        return null;
    }

    private static boolean isBridge(NetworkInterface networkInterface) {
        try {
            String interfaceName = networkInterface.getName();
            File file = new File("/sys/class/net/" + interfaceName + "/bridge");
            return file.exists();
        } catch (SecurityException ignored) {
        }
        return false;
    }

    /**
     * @link org.apache.rocketmq.common.BrokerConfig#localHostName
     */
    public static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }

        return "DEFAULT_BROKER";
    }
}
