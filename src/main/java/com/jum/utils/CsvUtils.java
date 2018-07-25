package com.jum.utils;

import au.com.bytecode.opencsv.CSVReader;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CsvUtils {

    private static final Logger logger = LoggerFactory.getLogger(CsvUtils.class);

    public static List<DataMap> getFromCsv(String csvPath) {
        return getFromCsv(csvPath, getAllColumnName(csvPath));
    }

    public static List<DataMap> getFromCsv(String csvPath, List<String> keys) {
        CSVReader csvr = null;
        List<DataMap> ldm = new ArrayList();
        try {
            csvr = new CSVReader(new FileReader(csvPath));
            List tableList = csvr.readAll();
            String[] cloumName = (String[]) tableList.get(0);
            if (CollectionUtils.isEmpty(keys)) {
                keys = Arrays.asList(cloumName);
            }
            Map<String, Integer> keyIds = getKeyIds(keys, cloumName);
            if (CollectionUtils.isEmpty(keyIds)) {
                logger.error("传入的key与csv文件中的列名不匹配");
                return ldm;
            }
            for (int i = 1; i < tableList.size(); i++) {
                String[] cloumValue = (String[]) tableList.get(i);
                if (cloumValue.length > 0) {
                    DataMap dm = new DataMap();
                    for (int j = 0; j < keys.size(); j++) {
                        dm.put(keys.get(j), cloumValue[((Integer) keyIds.get(keys.get(j))).intValue()]);
                    }
                    ldm.add(dm);
                }
            }
            return ldm;
        } catch (IOException e) {
            logger.error("File " + csvPath + "don't exist, or exceptions occurs when open this file.", e);
        }
        return ldm;
    }

    private static Map<String, Integer> getKeyIds(List<String> keys, String[] cloumName) {
        Map<String, Integer> keyIds = new HashMap();
        if ((CollectionUtils.isEmpty(keys)) || (ArrayUtils.isEmpty(cloumName))) {
            logger.error("传入的key为空或者csv文件的列名为空");
            return keyIds;
        }
        if (keys.size() > cloumName.length) {
            logger.error("传入的key个数大于csv文件中的所有列，请仔细核对");
            return keyIds;
        }
        for (String key : keys) {
            for (int j = 0; j < cloumName.length; j++) {
                if (key.equalsIgnoreCase(cloumName[j])) {
                    keyIds.put(key, Integer.valueOf(j));
                    break;
                }
            }
        }
        if (CollectionUtils.isEmpty(keyIds)) {
            logger.error("根据传入的key，所有key无法从csv文件中找到对应的列名，请仔细核对");
            return keyIds;
        }
        if (keyIds.size() != keys.size()) {
            logger.error("根据传入的key，存在某些key无法从csv文件中找到对应的列名，请仔细核对");
            return new HashMap();
        }
        return keyIds;
    }

    public static List<String> getAllColumnName(String csvPath) {
        CSVReader csvr = null;
        List<String> keys = new ArrayList();
        try {
            csvr = new CSVReader(new FileReader(csvPath));
            List tableList = csvr.readAll();
            String[] cloumName = (String[]) tableList.get(0);
            keys = Arrays.asList(cloumName);

            List<String> bigKeys = new ArrayList();
            for (String key : keys) {
                bigKeys.add(key.toUpperCase());
            }
            return bigKeys;
        } catch (IOException e) {
            logger.error("File " + csvPath + "don't exist, or exceptions occurs when open this file.", e);
        }
        return keys;
    }

    /**
     * csv文档信息转为json字符串
     * @param csvPath
     * @return
     */
    public static String csvToStr(String csvPath ){
        if (StringUtil.isEmpty(csvPath)) {
            logger.error("csv路径为空！");
            return "";
        }
        //从csv读数据
        List<DataMap> ldm = CsvUtils.getFromCsv(csvPath);
        if (CollectionUtils.isEmpty(ldm)) {
            logger.error("csv文件无数据内容！");
            return "";
        }
        return JSON.toJSONString(ldm);
    }

}
