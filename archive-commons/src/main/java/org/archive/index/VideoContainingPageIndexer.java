package org.archive.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.URIException;
import org.archive.hadoop.SortedTextFile;
import org.archive.url.AggressiveIAURLCanonicalizer;
import org.archive.url.HandyURL;
import org.archive.url.OrdinaryIAURLCanonicalizer;
import org.archive.url.URLCanonicalizer;
import org.archive.url.URLParser;

public class VideoContainingPageIndexer {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: java " + VideoContainingPageIndexer.class.getCanonicalName()
					+ " CRAWL_LOG_IN VIDEO_INDEX_OUT");
			System.err.println("Reads crawl log, writes unsorted video index by (aggressively canonicalized) containing page url");
			System.exit(1);
		}
		File crawlLogFile = new File(args[0]);
		
		File viaIndexFile = File.createTempFile("via-index-", ".txt");
		System.out.println("writing " + viaIndexFile);
		writeViaIndex(crawlLogFile, viaIndexFile);
		
		BufferedReader crawlLogIn = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), "UTF-8"));
		File videoIndexFile = new File(args[1]);
		PrintWriter videoIndexOut = new PrintWriter(videoIndexFile, "ASCII");
		
		System.out.println("writing " + videoIndexFile);
		Map<String, Integer> histo = writeUnsortedVideoIndex(videoIndexOut, crawlLogIn, viaIndexFile);
		logHisto(crawlLogFile, histo);
	}

	private static final URLCanonicalizer ordinary = new OrdinaryIAURLCanonicalizer();
	protected static String ordinary(String url) throws URIException {
	    HandyURL handyUrl = URLParser.parse(url);
	    ordinary.canonicalize(handyUrl);
	    return handyUrl.getURLString();
	}

	private static final URLCanonicalizer aggressive = new AggressiveIAURLCanonicalizer();
	protected static String aggressiveSchemelessSurt(String url) throws URIException {
	    HandyURL handyUrl = URLParser.parse(url);
	    aggressive.canonicalize(handyUrl);
	    return handyUrl.getSURTString(false);
	}

	protected static void logHisto(File crawlLogFile, Map<String, Integer> histo) {
		ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(histo.entrySet());
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		for (Entry<String, Integer> entry: list) {
			System.out.println(entry.getValue() + " " + entry.getKey() + " in " + crawlLogFile);
		}
	}

	protected static Map<String, Integer> writeUnsortedVideoIndex(
			PrintWriter videoIndexOut, BufferedReader crawlLogIn, File viaIndexFile)
			throws IOException {
		
		Map<String,Integer> histo = new HashMap<String, Integer>();
		for (String line = crawlLogIn.readLine(); line != null; line = crawlLogIn.readLine()) {
			String[] fields = line.split("\\s+");
			int fetchStatus = Integer.parseInt(fields[1]);
			String mimetype = fields[6];
			String discoveryPath = fields[4];
			if (fetchStatus > 0 && mimetype.matches("(?i)^video/.*$")) {
				String via = fields[5];
				String fetchD14 = fields[8];
				String sha1 = fields[9];
				if (sha1.startsWith("sha1:")) {
					sha1 = sha1.substring(5);
				}

				for (String containingPage: findContainingPages(viaIndexFile, via, discoveryPath)) {
					String containingPageKey = aggressiveSchemelessSurt(containingPage);
					String videoUrl = fields[3];
					String size = fields[2];
					
					videoIndexOut.println(containingPageKey 
							+ "\t" + videoUrl
							+ "\t" + fetchD14
							+ "\t" + mimetype
							+ "\t" + size
							+ "\t" + sha1);
				}

				if (histo.get(mimetype) != null) {
					histo.put(mimetype, histo.get(mimetype) + 1);
				} else {
					histo.put(mimetype, 1);
				}
			}
		}
		return histo;
	}

	/*
	# --- youtube watch page hop path ---
	# 16:29:37+nlevitt@aiapp202:/tmp/foo/002$ ~/workspace/ait-utils/scripts/util/heri_hoppath.pl crawl.log http://v3.cache2.c.youtube.com/videoplayback
	# 2011-05-17:  [200] http://www.youtube.com/watch?v=-POmiLIHyCs
	# 2011-05-17:   L: [200] http://www.youtube.com/watch?v=-o8tOMkZZqw&feature=related
	# 2011-05-17:    X: [200] http://www.youtube.com/watch?v=GC950eu3ZhA
	# 2011-05-17:     X: [302] http://v19.lscache6.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=904412,912502&algorithm=throttle-factor&itag=34&ipbits=8&burst=40&sver=3&signature=6F6D01795FB67361828683900242846ECA18F8B7.37FC92F4C0608675476FD262E2A7A98AD5D150B0&expire=1305626400&key=yt1&ip=207.0.0.0&factor=1.25&id=182f79d1ebb76610
	# 2011-05-17:      R: [200] http://v3.cache2.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=904412,912502&algorithm=throttle-factor&itag=34&ipbits=8&burst=40&sver=3&signature=6F6D01795FB67361828683900242846ECA18F8B7.37FC92F4C0608675476FD262E2A7A98AD5D150B0&expire=1305626400&key=yt1&ip=207.0.0.0&factor=1.25&id=182f79d1ebb76610&redirect_counter=1
	#
	# --- youtube embedded hop path (note watch page) ---
	# 16:30:49+nlevitt@aiapp202:/tmp/foo/003$ ~/workspace/ait-utils/scripts/util/heri_hoppath.pl crawl.log http://r2.cbf01t07.c.youtube.com/videoplayback
	# 2011-05-18:  [200] http://bomasanfrancisco.blogspot.com/2010/04/news-links-april-3-5-2010.html
	# 2011-05-18:   X: [200] http://www.youtube.com/watch?v=wBLxFe2NlOI
	# 2011-05-18:    X: [302] http://v12.lscache8.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=901039&algorithm=throttle-factor&itag=34&ipbits=8&burst=40&sver=3&signature=499C6327161FBFCDED143553AE63EA8FDE72FCCC.4FDBB3E7A064ADD86533B4791076483D4232AF8F&expire=1305763200&key=yt1&ip=207.0.0.0&factor=1.25&id=c012f115ed8d94e2
	# 2011-05-18:     R: [302] http://v4.cache7.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=901039&algorithm=throttle-factor&itag=34&ipbits=8&burst=40&sver=3&signature=499C6327161FBFCDED143553AE63EA8FDE72FCCC.4FDBB3E7A064ADD86533B4791076483D4232AF8F&expire=1305763200&key=yt1&ip=207.0.0.0&factor=1.25&id=c012f115ed8d94e2&redirect_counter=1
	# 2011-05-18:      R: [200] http://r2.cbf01t07.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=901039&algorithm=throttle-factor&itag=34&ipbits=8&burst=40&sver=3&signature=499C6327161FBFCDED143553AE63EA8FDE72FCCC.4FDBB3E7A064ADD86533B4791076483D4232AF8F&expire=1305763200&key=yt1&ip=207.0.0.0&factor=1.25&id=c012f115ed8d94e2&redirect_counter=1&st=ts
	#
	# --- another youtube watch page hop path ---
	# 16:36:33+nlevitt@aiapp202:/tmp/foo/004$ ~/workspace/ait-utils/scripts/util/heri_hoppath.pl crawl.log http://o-o.preferred.nuq04s10.v19.lscache7.c.youtube.com/videoplaybac
	# 2011-06-15:  [200] http://news.google.com/news/search?as_nsrc=new%20york%20times
	# 2011-06-15:   R: [200] http://news.google.com/news/section?pz=1&cf=all&ned=us&topic=t
	# 2011-06-15:    X: [200] http://www.youtube.com/watch?v=iNaIu7x-mBs
	# 2011-06-15:     X: [200] http://o-o.preferred.nuq04s10.v19.lscache7.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=907062,904535,911302&algorithm=throttle-factor&itag=34&ip=207.0.0.0&burst=40&sver=3&signature=2F0A7EF291A61387ECA392E0CC9821995B23467F.9C089010244B2DB83DE887CE2FCAA377E5F41D67&expire=1308160800&key=yt1&ipbits=8&factor=1.25&id=88d688bbbc7e981b
	# 
	# --- non-youtube ---
	# 16:38:23+nlevitt@aiapp202:/tmp/foo/004$ ~/workspace/ait-utils/scripts/util/heri_hoppath.pl crawl.log 'http://cdn.theguardian.tv/bc/281851582/281851582_993816604001_110615Fuk'
	# 2011-06-15:  [200] http://www.guardian.co.uk/
	# 2011-06-15:   L: [200] http://www.guardian.co.uk/world
	# 2011-06-15:    X: [200] http://cdn.theguardian.tv/bc/281851582/281851582_993816604001_110615Fukushima-16x9.mp4
	#
	# --- non-youtube ---
	# 17:39:43+nlevitt@aiapp202:/tmp/foo/005$ ~/workspace/ait-utils/scripts/util/heri_hoppath.pl crawl.log http://www.mass.gov/Eeohhs2/streaming/mcdhh_law_enforcement/have_you_been_drinking.mpg
	# 2011-06-11:  [200] http://www.mass.gov/
	# 2011-06-11:   R: [200] http://www.mass.gov/?pageID=mg2homepage&L=1&L0=Home&sid=massgov2
	# 2011-06-11:    L: [200] http://www.mass.gov/?pageID=mg2onlineservices&L=1&L0=Home&f=Home_more&sid=massgov2
	# 2011-06-11:     L: [200] http://www.mass.gov/?pageID=eohhs2subtopic&L=6&L0=Home&L1=Consumer&L2=Disability+Services&L3=Services+by+Type+of+Disability&L4=Deaf,+Late-Deafened,+and+Hard+of+Hearing&L5=Communication+Access,+Training+and+Technology+Services&sid=Eeohhs2
	# 2011-06-11:      L: [200] http://www.mass.gov/?pageID=eohhs2terminal&L=6&L0=Home&L1=Consumer&L2=Disability+Services&L3=Services+by+Type+of+Disability&L4=Deaf,+Late-Deafened,+and+Hard+of+Hearing&L5=Communication+Access,+Training+and+Technology+Services&sid=Eeohhs2&b=terminalcontent&f=mcdhh_c_law_enforcement_video&csid=Eeohhs2
	# 2011-06-11:       L: [200] http://www.mass.gov/Eeohhs2/streaming/mcdhh_law_enforcement/have_you_been_drinking.mpg
	#
	# --- non-youtube ---
	# 17:40:19+nlevitt@aiapp202:/tmp/foo/005$ ~/workspace/ait-utils/scripts/util/heri_hoppath.pl crawl.log http://streams.wgbh.org/online/gb/gb20110505_2.mp42011-06-11:  [200] http://www.mass.gov/
	# 2011-06-11:   R: [200] http://www.mass.gov/?pageID=mg2homepage&L=1&L0=Home&sid=massgov2
	# 2011-06-11:    L: [200] http://www.mass.gov/sao/
	# 2011-06-11:     E: [200] http://www.mass.gov/sao/osamenu.js
	# 2011-06-11:      X: [200] http://www.mass.gov/sao/archives.htm
	# 2011-06-11:       L: [200] http://www.mass.gov/sao/archivesmay2011.htm
	# 2011-06-11:        X: [200] http://streams.wgbh.org/online/gb/gb20110505_2.mp4
	 */
	/**
	 * Returns a list of url strings of pages that should be considered to
	 * contain the video, which in theory means that on the live web if you open
	 * the url, the video would be presented in the page. Examples are the
	 * youtube watch page and third party pages where the video is embedded.
	 * 
	 * The logic is confusing and hard to describe but I think it's correct:
	 * 
	 * <pre>
	 * for each hop (backwards from last)
	 *    if hop_type is not R
	 *       if containingPages is empty (this means we found the last non-redirect)
	 *        OR this hop url is not a youtube url (We know containingPages is not empty because of 
	 *                                              other part of the if statement. This means we already 
	 *                                              found a youtube watch page, and hop page we're 
	 *                                              inspecting must be a third-party page that either 
	 *                                              embeds or links to the youtube video.)
	 *          add hop to containingPages
	 *       if containingPages not empty and not youtube watch page
	 *          finished
	 * </pre>
	 * 
	 * @param viaIndexFile
	 * @param via
	 *            "via" of principal url
	 * @param discoveryPath
	 *            hop path e.g. LRXELE of video url
	 * @return list of pages that should be considered to contain the video, for
	 *         instance the youtube watch page and the page where the video was
	 *         embedded if any
	 * @throws IOException 
	 */
	protected static Iterable<String> findContainingPages(File viaIndexFile, String via,
			String discoveryPath) throws IOException {
		LinkedList<String> containingPages = new LinkedList<String>();

		String hopUrl = via;
		for (int i = discoveryPath.length() - 1; i >= 0; i--) {
			char hopType = discoveryPath.charAt(i);

			if (hopType != 'R') {
				if (containingPages.isEmpty() || !hopUrl.contains("youtube.com")) {
					containingPages.add(hopUrl);
				}

				if (!containingPages.isEmpty() && !hopUrl.contains("/youtube.com/watch?v=")) {
					break;
				}
			}

			// find next hop
			hopUrl = findVia(viaIndexFile, hopUrl);
			if (hopUrl == null) {
				// XXX if this happens it could indicate a problem with canonicalization?
				System.err.println("via not found in " + viaIndexFile + " for url " + hopUrl);
				break;
			}
		}

		return containingPages;
	}

	protected static String findVia(File viaIndexFile, String url) throws IOException {
		String canonicalUrl = ordinary(url);
		SortedTextFile sortedTextFile = new SortedTextFile(viaIndexFile);
		String matchingLine = sortedTextFile.firstMatchingRecord(canonicalUrl + ' ');
		if (matchingLine == null) {
			return null;
		}
		String via = matchingLine.substring(canonicalUrl.length() + 1);
		return via;
	}

	protected static void writeViaIndex(File crawlLogFile, File viaIndexFile) throws IOException {

		ProcessBuilder procBuilder = new ProcessBuilder("sort");
		procBuilder.environment().put("LC_ALL", "C");

		BufferedReader crawlLogIn;
		FileOutputStream viaIndexOut;
		try {
			crawlLogIn = new BufferedReader(new InputStreamReader(new FileInputStream(crawlLogFile), "UTF-8"));
			viaIndexOut = new FileOutputStream(viaIndexFile);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}

		Process proc = procBuilder.start();

		PrintWriter pipeToSort;
		try {
			pipeToSort = new PrintWriter(new OutputStreamWriter(proc.getOutputStream(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}

		InputStream pipeFromSort = proc.getInputStream();

		for (String line = crawlLogIn.readLine(); line != null; line = crawlLogIn.readLine()) {
			String[] fields = line.split("\\s+", 7);

			/* 
			 * (why we canonicalize these urls)
			 * 
			 * Note different canonicalization of the url starting with
			 * http://r2.cbf01t07.c.youtube.com/videoplayback?sparams... 
			 * in this crawl.log excerpt:
			 *
			 * 2011-05-18T17:53:49.778Z     1         67 dns:r2.cbf01t07.c.youtube.com XXRRP http://r2.cbf01t07.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=901039&algorithm=throttle-
			 * 2011-05-18T17:53:50.431Z   200         26 http://r2.cbf01t07.c.youtube.com/robots.txt XXRRP http://r2.cbf01t07.c.youtube.com/videoplayback?sparams=id,expire,ip,ipbits,itag,algorithm,burst,factor&fexp=901039&algor
			 * 2011-05-18T18:02:22.200Z   200   51494526 http://r2.cbf01t07.c.youtube.com/videoplayback?sparams=id%2Cexpire%2Cip%2Cipbits%2Citag%2Calgorithm%2Cburst%2Cfactor&fexp=901039&algorithm=throttle-factor&itag=34&ipbits=
			 */

			String url = ordinary(fields[3]);
			String via = ordinary(fields[5]);
			pipeToSort.println(url + ' ' + via);
		}

		pipeToSort.close();

		// finish even if it blocks
		byte[] buf = new byte[4096];
		for (int n = pipeFromSort.read(buf); n >= 0; n = pipeFromSort.read(buf)) { 
			viaIndexOut.write(buf, 0, n);
		}
		pipeFromSort.close();
		viaIndexOut.close();
	}

}
