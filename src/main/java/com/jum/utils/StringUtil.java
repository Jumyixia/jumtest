package com.jum.utils;

public class StringUtil {

    //不能实例化
    private StringUtil() {}

    /**
     * 检查指定的字符串是否为空。
     * <ul>
     * <li>SysUtils.isEmpty(null) = true</li>
     * <li>SysUtils.isEmpty("") = true</li>
     * <li>SysUtils.isEmpty("   ") = true</li>
     * <li>SysUtils.isEmpty("abc") = false</li>
     * </ul>
     *
     * @param value 待检查的字符串
     * @return true/false
     */
    public static boolean  isEmpty(String value){
        int strLen;
        if(value == null || (strLen = value.length()) == 0){
            return true;
        }
        for(int i =0 ;i < strLen; i++){
            if((Character.isWhitespace(value.charAt(i)) == false)){
                return false;
            }
        }

        return true;
    }

}