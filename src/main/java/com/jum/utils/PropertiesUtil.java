package com.jum.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {
    public PropertiesUtil() {
    }

    public static String readPropertyValue(String fileName, String key) throws IOException {
        Properties props = new Properties();
        InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
        props.load(in);
        String value = props.getProperty(key);
        return value;
    }

    public static Map<String, String> readProperties(String fileName) throws IOException {
        Map<String, String> properties = new HashMap();
        Properties props = new Properties();
        InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
        props.load(in);
        Enumeration en = props.propertyNames();

        while(en.hasMoreElements()) {
            String key = (String)en.nextElement();
            String property = props.getProperty(key);
            properties.put(key, property);
        }

        return properties;
    }
}