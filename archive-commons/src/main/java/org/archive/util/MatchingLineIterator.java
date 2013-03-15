package org.archive.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;

/* commons-io LineIterator is retarded and declares member vars private, forcing us to copy & paste the whole damn class */
public class MatchingLineIterator implements Iterator<String> {

    protected final BufferedReader bufferedReader;
    protected String cachedLine;
    protected boolean finished = false;
	protected String prefix;

	public MatchingLineIterator(String prefix, Reader reader) {
        if (reader == null) {
            throw new IllegalArgumentException("Reader must not be null");
        }
        if (reader instanceof BufferedReader) {
            bufferedReader = (BufferedReader) reader;
        } else {
            bufferedReader = new BufferedReader(reader);
        }
        
		this.prefix = prefix;
	}

    public boolean hasNext() {
        if (cachedLine != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            try {
                while (true) {
                    String line = bufferedReader.readLine();
                    if (line == null || !line.startsWith(prefix)) {
                        finished = true;
                        return false;
                    } else {
                    	cachedLine = line;
                    	return true;
                    }
                }
            } catch(IOException ioe) {
                close();
                throw new IllegalStateException(ioe.toString());
            }
        }
    }

    public String next() {
        return nextLine();
    }

    public String nextLine() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }
        String currentLine = cachedLine;
        cachedLine = null;
        return currentLine;        
    }

    public void close() {
        finished = true;
        IOUtils.closeQuietly(bufferedReader);
        cachedLine = null;
    }

    public void remove() {
        throw new UnsupportedOperationException("Remove unsupported on LineIterator");
    }

}
