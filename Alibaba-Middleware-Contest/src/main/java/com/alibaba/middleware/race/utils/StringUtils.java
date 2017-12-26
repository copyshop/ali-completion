package com.alibaba.middleware.race.utils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.alibaba.middleware.race.entity.KV;
import com.alibaba.middleware.race.entity.Row;

/**
 * Utils of string
 */
public class StringUtils {

	public static String removeEnd(String str, String remove) {
		if (str == null || remove == null) {
			return str;
		}
		if (str.endsWith(remove)) {
			return str.substring(0, str.length() - remove.length());
		}
		return str;
	}

	/**
	 * 改进版的切割方法 使用indexOf和循环来切分字符串，该方法针对普通文件的切割为Row
	 * 
	 * @param line
	 * @param splitch
	 * @return
	 */
	public static Row createKVMapFromLine(String line, char splitch) {
		int splitIndex;
		if (line.charAt(line.length() - 1) == splitch) {
			line = line.substring(0, line.length() - 1);
		}
		Row kvMap = new Row();
		String splitted;
		splitIndex = line.indexOf(splitch);
		int p;
		String key;
		String value;
		while (splitIndex != -1) {
			splitted = line.substring(0, splitIndex);
			line = line.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			value = splitted.substring(p + 1);
			KV kv = new KV(key, value);
			kvMap.put(kv.key(), kv);
			splitIndex = line.indexOf(splitch);
		}
		splitted = line;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		value = splitted.substring(p + 1);
		KV kv = new KV(key, value);
		kvMap.put(kv.key(), kv);
		return kvMap;
	}
	
	public static  String[] getIndexInfo(String lineSegment) {
		String[] indexInfo = new String[3];
		for(int i = 0; i<= 1; i++) {
			int p = lineSegment.indexOf(' ');
			indexInfo[i] = lineSegment.substring(0, p);
			lineSegment = lineSegment.substring(p + 1);
		}
		indexInfo[2] = lineSegment;
		return indexInfo;
	}

	/**
	 * 将一行切割为HashMap 用于查询1和join操作
	 * @param longLine
	 * @param splitch
	 * @return
	 */
	public static HashMap<String, String> createMapFromLongLine(String longLine, char splitch) {
		int splitIndex;
		if (longLine.charAt(longLine.length() - 1) == splitch) {
			longLine = longLine.substring(0, longLine.length() - 1);
		}
		String splitted;
		splitIndex = longLine.indexOf(splitch);
		int p;
		String key;
		String value;
		HashMap<String, String> result = new HashMap<>(128);
		while (splitIndex != -1) {
			splitted = longLine.substring(0, splitIndex);
			longLine = longLine.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			value = splitted.substring(p + 1);
			result.put(key, value);

			splitIndex = longLine.indexOf(splitch);
		}
		splitted = longLine;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		value = splitted.substring(p + 1);
		result.put(key, value);
		return result;
	}

	/**
	 * 特定前缀头切割的map 用于query2查询一个buyer的order信息
	 * @param longLine
	 * @param k
	 * @param splitch
	 * @return
	 */
	public static HashMap<String, String> createMapFromLongLineWithPrefixKey(String longLine, String k, char splitch) {
		int splitIndex;
		if (longLine.charAt(longLine.length() - 1) == splitch) {
			longLine = longLine.substring(0, longLine.length() - 1);
		}
		String splitted;
		splitIndex = longLine.indexOf(splitch);
		int p;
		String key;
		HashMap<String, String> result = new HashMap<>(100);

		while (splitIndex != -1) {
			splitted = longLine.substring(0, splitIndex);
			longLine = longLine.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			if (key.startsWith(k)) {
				result.put(key, splitted.substring(p + 1));
			}
			splitIndex = longLine.indexOf(splitch);
		}
		splitted = longLine;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		if (key.startsWith(k)) {
			result.put(key, splitted.substring(p + 1));
		}

		return result;
	}

	/**
	 * 取出一行中key固定的值组成list,针对index的offSet
	 * @param longLine
	 * @param k
	 * @param splitch
	 * @return
	 */
	public static List<String> createListFromLongLineWithKey(String longLine, String k, char splitch) {
		int splitIndex;
		if (longLine.charAt(longLine.length() - 1) == splitch) {
			longLine = longLine.substring(0, longLine.length() - 1);
		}
		String splitted;
		splitIndex = longLine.indexOf(splitch);
		int p;
		String key;
		List<String> result = new ArrayList<>(100);

		while (splitIndex != -1) {
			splitted = longLine.substring(0, splitIndex);
			longLine = longLine.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			if (key.startsWith(k)) {
				result.add(splitted.substring(p + 1));
			}
			splitIndex = longLine.indexOf(splitch);
		}
		splitted = longLine;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		if (key.startsWith(k)) {
			result.add(splitted.substring(p + 1));
		}
		return result;
	}

	/**
	 * Converts an array of bytes into a string with the indicated character set
	 * enumeration
	 * 
	 * @param array
	 * @param charsetName
	 * @return
	 */
	public static String convert(byte[] array, String charsetName) {
		if (array == null || array.length == 0) {
			System.err.println("As the input byte array is empty for conversion, empty string will be returned.");
			return new String();
		} else
			return new String(array, Charset.forName(charsetName));
	}
	/**
	 * 将字符串转换为long或者double，无法转换的之后返回null
	 * @param value
	 * @return
	 */
	public static Object parseStringToNumber(String value) {
		
		try {
			long longValue = Long.parseLong(value);
			return longValue;
		} catch(NumberFormatException e) {
			
		}
		try {
			double doubleValue = Double.parseDouble(value);
			return doubleValue;
		} catch(NumberFormatException e) {
			
		}
		return null;
	}
}
