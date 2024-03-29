package org.kendar.janus.utils;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Properties;

public class LocalProperties {
    private static final LoggerWrapper log = LoggerWrapper.getLogger(LocalProperties.class);
    private static Properties localProperties;

    public static Properties build(String toMerge){
        if(localProperties==null){
            try {
                localProperties = new Properties();
                InetAddress address = InetAddress.getLocalHost();
                localProperties.put("janus.client.address", address.getHostAddress());
                localProperties.put("janus.client.name", address.getHostName());
                if(toMerge!=null) {
                    Arrays.stream(toMerge.split(";"))
                            .forEach(key -> {
                                if (key != null && !key.trim().isEmpty()) {
                                    String value = System.getProperty(key);
                                    if (value != null) {
                                        localProperties.put(key, value);
                                    }
                                }
                            });
                }
            }catch (Exception ex){
                log.info("Error loading client properties");
            }

        }
        return localProperties;
    }
}
