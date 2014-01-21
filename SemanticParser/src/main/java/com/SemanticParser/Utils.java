package com.SemanticParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Utils {

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
