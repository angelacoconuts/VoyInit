package com.SemanticParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class Utils {

	public static <K, V extends Comparable<? super V>> List<K> getMostFrequentItemInMap(
			Map<K, V> map, int topResultToReturn) {
		
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		List<K> topResult = new ArrayList<K>();
		
		Collections.sort(list,
				Collections.reverseOrder(new Comparator<Map.Entry<K, V>>() {
					public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
						return (o1.getValue()).compareTo(o2.getValue());
					}
				}));

		for (int i = 0 ; i < topResultToReturn ; i++){
			topResult.add(list.get(i).getKey());
		//	App.logger.info("Top "+i+" result: "+list.get(i).getValue());
		}

		return topResult;
	}

	public static String[] ngrams(String str, int ngramDegree) {

		if (str.length() < ngramDegree)
			return new String[] { str };

		char[] chars = str.toCharArray();
		final int resultLength = chars.length - ngramDegree + 1;
		String[] ngramArray = new String[resultLength];

		for (int i = 0; i < resultLength; i++)
			ngramArray[i] = new String(chars, i, ngramDegree);

		return ngramArray;

	}

	public static List<String> splitNgrams(String str, int ngramDegree) {

		List<String> ngramList = new ArrayList<String>();

		for (String string : StringUtils.split(str))
			ngramList.addAll(Arrays.asList(ngrams(string, ngramDegree)));

		return ngramList;
	}

	public static List<String> splitNgrams(String[] tokens, int ngramDegree) {

		List<String> ngramList = new ArrayList<String>();

		for (String string : tokens)
			ngramList.addAll(Arrays.asList(ngrams(string, ngramDegree)));

		return ngramList;
	}
}
