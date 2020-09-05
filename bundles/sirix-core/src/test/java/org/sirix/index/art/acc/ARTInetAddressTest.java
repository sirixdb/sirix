//package org.sirix.index.art.acc;
//
//import org.sirix.index.art.AbstractNavigableMapShortTest;
//import org.sirix.index.art.AdaptiveRadixTree;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.*;
//import java.util.concurrent.ThreadLocalRandom;
//
//public class ARTInetAddressTest extends AbstractNavigableMapShortTest<InetAddress, String> {
//	private static final Data[] data = load();
//	private static final InetAddress[] sampleKeys = ips();
//	private static final String[] sampleValues = countries();
//	private static final String[] newSampleValues = reverseSampleValues();
//	private static final InetAddress[] otherKeys = generateOtherKeys();
//	private static final String[] otherSampleValues = generateOtherSampleValues();
//
//	private static String[] generateOtherSampleValues() {
//		int n = 10;
//		String[] otherKeys = new String[n];
//		for (int i = 0; i < 10; i++) {
//			otherKeys[i] = UUID.randomUUID().toString();
//		}
//		return otherKeys;
//	}
//
//	private static class Data {
//		private final InetAddress address;
//		private final String country;
//
//		Data(InetAddress address, String country) {
//			this.address = address;
//			this.country = country;
//		}
//	}
//
//	private static InetAddress[] generateOtherKeys() {
//		int n = 10;
//		InetAddress[] otherKeys = new InetAddress[n];
//		for (int i = 0; i < 10; i++) {
//			Random r = ThreadLocalRandom.current();
//			String address = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
//			try {
//				otherKeys[i] = InetAddress.getByName(address);
//			}
//			catch (UnknownHostException e) {
//				throw new RuntimeException(e);
//			}
//		}
//		return otherKeys;
//	}
//
//	private static String[] reverseSampleValues() {
//		String[] keys = new String[sampleValues.length];
//		for (int i = 0; i < keys.length; i++) {
//			keys[i] = new StringBuilder(sampleValues[i]).reverse().toString();
//		}
//		return keys;
//	}
//
//	private static InetAddress[] ips() {
//		return Arrays.stream(data).map(d -> d.address).toArray(InetAddress[]::new);
//	}
//
//	private static String[] countries() {
//		return Arrays.stream(data).map(d -> d.country).toArray(String[]::new);
//	}
//
//	private static Data[] load() {
//		try (BufferedReader br = new BufferedReader(new InputStreamReader(ARTInetAddressTest.class
//				.getResourceAsStream("/art-index/ip-by-country.csv")))) {
//			String line = br.readLine(); // read column header
//			List<Data> l = new ArrayList<>();
//			while ((line = br.readLine()) != null) {
//				String[] values = line.split(",");
//				String start = values[0].replace("\"", "");
//				String country = values[5].replace("\"", "");
//				// System.out.println(start + ", " + end + ", " + country);
//				InetAddress startAddress = InetAddress.getByName(start);
//				l.add(new Data(startAddress, country));
//			}
//			return l.toArray(new Data[0]);
//		}
//		catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public ARTInetAddressTest(String testName) {
//		super(testName);
//	}
//
//	@Override
//	public InetAddress[] getSampleKeys() {
//		return sampleKeys;
//	}
//
//	@Override
//	public String[] getSampleValues() {
//		return sampleValues;
//	}
//
//	@Override
//	public String[] getNewSampleValues() {
//		return newSampleValues;
//	}
//
//	@Override
//	public InetAddress[] getOtherKeys() {
//		return otherKeys;
//	}
//
//	@Override
//	public String[] getOtherValues() {
//		return otherSampleValues;
//	}
//
//	@Override
//	public NavigableMap<InetAddress, String> makeObject() {
//		return new AdaptiveRadixTree<>(InetAddress::getAddress);
//	}
//
//	@Override
//	public NavigableMap<InetAddress, String> makeConfirmedMap() {
//		return new TreeMap<>(InetAddressComparator.INSTANCE);
//	}
//
//	enum InetAddressComparator implements Comparator<InetAddress> {
//		INSTANCE;
//
//		@Override
//		public int compare(InetAddress o1, InetAddress o2) {
//			byte[] b1 = o1.getAddress();
//			byte[] b2 = o2.getAddress();
//			for (int i = 0; i < 4; i++) {
//				int res = Byte.compareUnsigned(b1[i], b2[i]);
//				if (res != 0) {
//					return res;
//				}
//			}
//			return 0;
//		}
//	}
//}
