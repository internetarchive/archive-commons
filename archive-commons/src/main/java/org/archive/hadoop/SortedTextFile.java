package org.archive.hadoop;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.archive.util.MatchingLineIterator;

/**
 * Binary search to find first and last matches in text file sorted by line.
 * File can be in HDFS or on local filesystem. Assumes the file is in UTF-8, but
 * operates on bytes only by design to emulate behavior of unix commands with
 * LC_ALL=C.
 * 
 * <p>
 * man sort(1) says "*** WARNING *** The locale specified by the environment
 * affects sort order. Set LC_ALL=C to get the traditional sort order that uses
 * native byte values."
 */
public class SortedTextFile {
    protected FileSystem fs = null;
    protected Path path = null;

	public SortedTextFile(File parent, String child) {
	    // XXX path separator? hadoop always uses slash but File is system-dependent... use File.getURI()?
	    this(localFs(), new Path(parent.getPath(), child));
	}

	public SortedTextFile(String localPath) {
	    // XXX path separator? hadoop always uses slash but File is system-dependent... use File.getURI()?
	    this(localFs(), new Path(localPath));
	}

	public SortedTextFile(File file) {
        // XXX path separator? hadoop always uses slash but File is system-dependent... use File.getURI()?
        this(localFs(), new Path(file.getPath()));
	}

	public SortedTextFile(FileSystem fs, Path path) {
	   this.fs = fs;
	   this.path = path;
    }

    /** 
	 * Sets contents of {@code lineBuf} to the line that {@code pos} is in the middle of. 
	 * @return position of beginning of line
	 * @throws IOException 
	 */
	protected long getLineAtOffset(FSDataInputStream fh, long pos, ByteBuff lineBuf) throws IOException {
		lineBuf.reset();

		long linePos;
		int b = -1;
		
		for (linePos = pos - 1; linePos >= 0 && b != '\n'; linePos--) {
			fh.seek(linePos);
			b = fh.read();
		}
		
		if (b == '\n') {
			// linePos is pointing to byte before '\n'
			linePos += 2;
		} else if (linePos < 0) {
			// either we never seeked or fh.read() ate first byte of file 
			linePos = 0;
			fh.seek(linePos);
		}

		for (b = fh.read(); b != '\n' && b != -1; b = fh.read()) {
			lineBuf.append(b);
		}

		return linePos;
	}
	
	/** This is implemented for a prefix search, so if it matches up to prefixSearchBytes.length, returns 0. */
	protected int prefixCompare(byte[] prefixSearchBytes, ByteBuff lineBuf) {
		for (int i = 0; i < prefixSearchBytes.length; i++) {
			if (lineBuf.byteAt(i) == '\n' || i >= lineBuf.size()) {
				// matches up to end of the *line* but search string continues, therefore is "greater than" prefix
				return 1;
			} else if (prefixSearchBytes[i] > lineBuf.byteAt(i)) {
				return 1;
			} else if (prefixSearchBytes[i] < lineBuf.byteAt(i)) {
				return -1;
			}
		}

		// if we get here, it's a match
		return 0; 
	}
	
	protected long findFirstOffset(FSDataInputStream fh, FileStatus stat, String str) throws IOException {
		return findOffset(fh, stat, false, str);
	}
	
	protected long findLastOffset(FSDataInputStream fh, FileStatus stat, String str) throws IOException {
		return findOffset(fh, stat, true, str);
	}
	
	static class ByteBuff extends ByteArrayOutputStream {
		// hooray for protected-scope members
		public int byteAt(int i) {
			if (i < 0 || i >= this.count) {
				return -1;
			} else {
				return this.buf[i];
			}
		}

		// buffer instead of stream semantic
		public void append(int b) {
			write(b);
		}
		
		// ha ha!!! for variables thing in debugger
		public String toString() {
			try {
				return new String(Arrays.copyOfRange(this.buf, 0, this.count), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e); 
			}
		}
	}
	
	// findLast=false means find first
	// returns -1 if no match
	protected long findOffset(FSDataInputStream fh, FileStatus stat, boolean findLast, String prefixSearchString) throws IOException {
		byte[] prefixSearchBytes = prefixSearchString.getBytes("UTF-8");
		
		long left = 0l;
		long right = stat.getLen() - 1;

		ByteBuff lineBuf = new ByteBuff();

		while (right - left > 0) {
			long pos = (left + right + 1) / 2;
			getLineAtOffset(fh, pos, lineBuf);
			
			int cmp = prefixCompare(prefixSearchBytes, lineBuf);
			if (cmp > 0 || (cmp == 0 && findLast)) {
				left = pos + 1;
			} else if (cmp < 0 || (cmp == 0 && !findLast)) {
				right = pos - 1;
			} else { 
				throw new RuntimeException("umm... impossiblish?");
			}
		}

		// if !findLast, we are on the first byte of the line, or the preceding newline;
		// else if findLast, we're on the last byte of the line, or the first byte of the next line
		long pos = (findLast ? -1 : 1) + (left + right + 1) / 2;
		long linePos = getLineAtOffset(fh, pos, lineBuf);
		
		if (prefixCompare (prefixSearchBytes, lineBuf) == 0) {
			return linePos; // match found
		} else {
			return -1;
		}
	}	

	/**
	 * Returns iterator over lines matching prefix.
	 * @param prefix
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public MatchingLineIterator getMatchingLineIterator(final String prefix)
			throws FileNotFoundException, IOException {
	    FileStatus stat = fs.getFileStatus(path);
	    FSDataInputStream fh = fs.open(path);
		
		long offset = findFirstOffset(fh, stat, prefix);
		if (offset < 0) {
			return null;
		}
		
		fh.seek(offset);
		
		InputStreamReader reader = new InputStreamReader(fh, "UTF-8");
		return new MatchingLineIterator(prefix, reader);
	}
	
	public long countBytesOfAllMatches(String str) throws IOException {
        FileStatus stat = fs.getFileStatus(path);
        FSDataInputStream fh = fs.open(path);

		long start = findFirstOffset(fh, stat, str);
		if (start < 0) {
			return 0;
		}
		long end = findLastOffset(fh, stat, str);
		fh.seek(end);
		for (int b = fh.read(); b != '\n' && b != -1; b = fh.read()) {
			end++;
		}
		
		return end - start + 1;
	}

	
	public static void main(String[] args) throws IOException {
		if(args.length < 2) {
			System.err.println("Usage: java " + SortedTextFile.class.getCanonicalName() + " PREFIX FILE");
			System.exit(1);
		}
		
		SortedTextFile sortedTextFile = new SortedTextFile(args[1]);
		
//		RandomAccessFile fh = new RandomAccessFile(f.file, "r");
//		System.out.println("first offset: " + f.findFirstOffset(fh, args[0]));
//		System.out.println("last offset: " + f.findLastOffset(fh, args[0]));
//		System.out.flush();
		
		System.out.println("firstMatchingRecord: " + sortedTextFile.firstMatchingRecord(args[0]));
		System.out.println("lastMatchingRecord: " + sortedTextFile.lastMatchingRecord(args[0]));
		System.out.println("total bytes: " + sortedTextFile.countBytesOfAllMatches(args[0]));
		
		MatchingLineIterator iter = sortedTextFile.getMatchingLineIterator(args[0]);
		while (iter != null && iter.hasNext()) {
			String line = iter.next();
			System.out.println(line);
		}
	}

	@Override
	public String toString() {
	    return fs.getUri().resolve(path.toString()).toString();
	}

	protected String matchingRecord(boolean findLast, String searchPrefix) throws IOException {
	    FileStatus stat = fs.getFileStatus(path);
	    FSDataInputStream fh = fs.open(path);

		long offset = findOffset(fh, stat, findLast, searchPrefix);
		
		if (offset < 0) {
			return null;
		}
		
		fh.seek(offset);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fh, "UTF-8"));
		return br.readLine();
	}

	public String firstMatchingRecord(String searchPrefix) throws IOException {
		return matchingRecord(false, searchPrefix);
	}

	public String lastMatchingRecord(String searchPrefix) throws IOException {
		return matchingRecord(true, searchPrefix);
	}
	
	protected static LocalFileSystem localFs;
	public static LocalFileSystem localFs() {
	    if (localFs == null) {
	        Configuration conf = new Configuration();
	        try {
                localFs = FileSystem.getLocal(conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
	    }
	    
	    return localFs;
	}

}
