package com.clouditora.mq.broker;

import lombok.Data;

import java.io.File;

@Data
public class MessageStoreConfig {
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

}
