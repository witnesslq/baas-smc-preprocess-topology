package com.ai.baas.smc.preprocess.topology.core.util;

/**
 * 判断表达式是否包含
 * 
 * @author zhangbc
 *
 */
public  class IKin {

	public static boolean in(String a, String b) {
		if (b.startsWith("{")) {
			b = b.substring(1, b.length() - 1);
		}
		String[] bs = b.split(",");
		for (int i = 0; i < bs.length; i++) {
			if (bs[i].equals(a)) {
				return true;
			}
		}
		return false;
	}

}