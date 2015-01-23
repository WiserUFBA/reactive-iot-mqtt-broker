package io.github.giovibal.mqtt;

import java.nio.ByteBuffer;


/**
 * So how do we work around this lack of unsigned types? Well, you're probably
 * not going to like this... The answer is, you use the signed types that are
 * larger than the original unsigned type. I.e. use a short to hold an unsigned
 * byte, use a long to hold an unsigned int. (And use a char to hold an unsigned
 * short.). Yeah, this kinda sucks because now you're using twice as much
 * memory, but there really is no other solution. (Also bear in mind, access to
 * longs is not guaranteed to be atomic - although if you're using multiple
 * threads, you really should be using synchronization anyway.)
 */
public class ConversionUtility {
	final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

	// public static byte[] fromHexString(String hex) {
	// return fromHexString(hex, "");
	// }


	// public static String bytesToHex(byte[] bytes) {
	// char[] hexChars = new char[bytes.length * 2];
	// int v;
	// for (int j = 0; j < bytes.length; j++) {
	// v = bytes[j] & 0xFF;
	// hexChars[j * 2] = hexArray[v >>> 4];
	// hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	// }
	// return new String(hexChars);
	// }

	public static byte[] fromHexString(String hex) {
		hex = hex.replaceAll("[^A-Fa-f0-9]", "");
		final int len = hex.length();

		int block = 2;
		int rest = (block - (len % (block)));
		rest = rest == block ? 0 : rest;

		byte[] out = null;
		if (len > 1) {
			out = new byte[(len + rest) / (2)];

			int counter = 0;

			for (int i = 0; i < len; i += (2)) {
				int h = hexToBin(hex.charAt(i));
				int l = hexToBin(hex.charAt(i + 1));
				if (h == -1 || l == -1) {
					throw new IllegalArgumentException("contains illegal character for hexBinary: " + hex);
				}

				out[counter++] = (byte) (h * 16 + l);
			}
		}
		return out;
	}

	// public static byte[] fromHexString(String hex, String hexSeparator) {
	// // hex = hex.replaceAll("[^A-Fa-f0-9]", "");
	// // char[] hexChars = hex.toCharArray();
	//
	// return DatatypeConverter.parseHexBinary(hex);
	// }

	// public static byte[] fromHexString(String s) {
	// return ConversionUtility.fromHexString(s, "");
	// }

	@Deprecated
	public static byte[] fromHexString(String hex, String hexSeparator) {
		return fromHexString(hex);
		// hex = hex.replaceAll("[^A-Fa-f0-9]", hexSeparator);
		// hexSeparator = hexSeparator != null ? hexSeparator : "";
		// final int len = hex.length();
		// final int hs = hexSeparator.length();
		//
		// int block = 2 + hs;
		// int rest = (block - (len % (block)));
		// rest = rest == block ? 0 : rest;
		//
		// byte[] out = null;
		// if (len > 1) {
		// out = new byte[(len + rest) / (2 + hs)];
		//
		// int counter = 0;
		//
		// for (int i = 0; i < len; i += (2 + hs)) {
		// int h = hexToBin(hex.charAt(i));
		// int l = hexToBin(hex.charAt(i + 1));
		// if (h == -1 || l == -1) {
		// throw new IllegalArgumentException("contains illegal character for hexBinary: " + hex);
		// }
		//
		// out[counter++] = (byte) (h * 16 + l);
		// }
		// }
		// return out;
	}

	public static String getHex(byte b) {
		return getHex(b, true);
	}

	public static String getHex(byte b, boolean prefix) {
		return String.format(prefix ? "%#04x" : "%02x", b);
	}

	public static String getHex(byte[] ba) {
		return getHex(ba, 0, ba.length);

	}

	public static String getHex(byte[] ba, int offset, int length) {
		return getHex(ba, offset, length, true);
	}
	
	public static String getHex(byte[] ba, int offset, int length, boolean prefix) {
		return (prefix ? "0x" : "") + toHexString(ba, offset, length, "");
	}

	public static String getHex(char b) {
		return getHex(b, true);
	}

	public static String getHex(char b, boolean prefix) {
		return String.format(prefix ? "%#04x" : "%02x", b);
	}

	public static String getHex(double b) {
		return getHex(b, true);
	}

	public static String getHex(double b, boolean prefix) {
		return String.format(prefix ? "%#018x" : "%016x", b);
	}

	public static String getHex(float b) {
		return getHex(b, true);
	}

	public static String getHex(float b, boolean prefix) {
		return String.format(prefix ? "%#010x" : "%08x", b);
	}

	public static String getHex(int b) {
		return getHex(b, true);
	}

	public static String getHex(int b, boolean prefix) {
		return String.format(prefix ? "%#010x" : "%08x", b);
	}

	public static String getHex(long b) {
		return getHex(b, true);
	}

	public static String getHex(long b, boolean prefix) {
		return String.format(prefix ? "%#018x" : "%016x", b);
	}

	public static String getHex(short b) {
		return getHex(b, true);
	}

	public static String getHex(short b, boolean prefix) {
		return String.format(prefix ? "%#06x" : "%04x", b);
	}

	public static String getUnsigned(byte b) {
		int result = b & 0xFF;
		return Integer.toString(result);
	}

	public static String getUnsigned(int b) {
		long result = b & 0xFFFFFFFF;
		return Long.toString(result);
	}

	public static String getUnsigned(short b) {
		int result = b & 0xFFFF;
		return Integer.toString(result);
	}

	private static int hexToBin(char ch) {
		if ('0' <= ch && ch <= '9') {
			return ch - '0';
		}
		else if ('A' <= ch && ch <= 'F') {
			return ch - 'A' + 10;
		}
		else if ('a' <= ch && ch <= 'f') {
			return ch - 'a' + 10;
		}
		return -1;
	}



	public static String toHexString(byte[] aByteArray) {
		return toHexString(aByteArray, 0, aByteArray.length, "");
	}

	public static String toHexString(byte[] bytes, int length, int offset, String separator) {
		char[] hexChars = new char[bytes.length * (2 + separator.length()) - separator.length()];
		char[] separatorChars = separator.toCharArray();
		int v;
		int step = 2 + separatorChars.length;

		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;

			hexChars[j * step] = hexArray[v >>> 4];
			hexChars[j * step + 1] = hexArray[v & 0x0F];

			if (bytes.length - j > 1) {
				for (int s = 0; s < separatorChars.length; s++) {
					hexChars[j * step + 2 + s] = separatorChars[s];
				}
			}
		}

		return new String(hexChars);
	}

	public static String toHexString(byte[] aByteArray, String hexSeparator) {
		return toHexString(aByteArray, 0, aByteArray.length, hexSeparator);
	}

	public static String toHexString(ByteBuffer aByteBuffer) {
		return toHexString(aByteBuffer.array(), aByteBuffer.position(), aByteBuffer.capacity(), "");
	}

	public static String toHexString(ByteBuffer aByteBuffer, String hexSeparator) {
		return toHexString(aByteBuffer.array(), aByteBuffer.position(), aByteBuffer.capacity(), hexSeparator);
	}

	public static short unsigned(byte b) {
		short result = (short) (b & 0xFF);
		return result;
	}

	public static long unsigned(int b) {
		long result = (long)(b & 0xFFFFFFFFL);
		return result;
	}

	public static int unsigned(short b) {
		int result = b & 0xFFFF;
		return result;
	}
	
	
	public static void main(String[] args) {
		System.out.println(" " + unsigned((byte)0xff));
		System.out.println(" " + unsigned((short)0xffff));
		System.out.println(" " + unsigned((int)0xffffffff));
//		System.out.println(" " + unsigned(0xffff0001));
	}

}
