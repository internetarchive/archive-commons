package org.archive.util.binsearch.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.archive.util.binsearch.SeekableLineReader;
import org.archive.util.binsearch.SeekableLineReaderFactory;

/**
 * A SeekableLineReaderFactory that opens a file ONCE at construction.
 * 
 * Should be able to re-use the same FileChannel across threads, and might be
 * more efficient than the RandomAccessFileSeekableLineReaderXXX classes.
 * 
 * @author brad
 *
 */
public class NIOSeekableLineReaderFactory implements SeekableLineReaderFactory {
	private File file;
	private FileChannel fc;
	private RandomAccessFile raf;
	private int blockSize;
	
	//protected int lastModified;
	
	public NIOSeekableLineReaderFactory(File file, int blockSize) throws IOException {
		this.file = file;
		this.blockSize = blockSize;
		this.raf = new RandomAccessFile(file,"r");
		this.fc = raf.getChannel();
	}
	public NIOSeekableLineReaderFactory(File file) throws IOException {
		this(file, BINSEARCH_BLOCK_SIZE);
	}
	public SeekableLineReader get() throws IOException {
		
//		synchronized (this) {
//			if (file.lastModified() != lastModified) {
//				close();
//				this.raf = new RandomAccessFile(file, "r");
//				this.fc = raf.getChannel();
//			}
//		}
		
		return new NIOSeekableLineReader(fc, blockSize);
	}
	public void close() throws IOException
	{
		if (raf != null) {
			raf.close();
		}
	}
	
	public long getModTime()
	{
		return file.lastModified();
	}
}
