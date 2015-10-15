package edu.uchicago.mpcs53013;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TIOStreamTransport;


/**
 * Simple class that makes it easy to write Thrift objects to disk.
 * @see http://joelpm.com/2009/02/05/thrift-writing-thrift-objects-to-disk-with-java.html
 * @author Joel Meyer
 */
public class ThriftWriter {
	  /** File to write to. */
	  protected final File file;
	  
	  /** For writing to the file. */
	  private BufferedOutputStream bufferedOut;

	  /** For binary serialization of objects. */
	  private TProtocol protocolOut;
	  
	  /**
	    * Constructor.
	    */
	  public ThriftWriter(File file) {
	    this.file = file;
	  }
	  
	  /**
	    * Open the file for writing.
	    */
	  public void open() throws FileNotFoundException {
	    bufferedOut = new BufferedOutputStream(new FileOutputStream(file), 2048);
	    protocolOut = new TJSONProtocol(new TIOStreamTransport(bufferedOut));
	  }
	  
	  /**
	    * Write the object to disk.
	    */
	  public void write(TBase t) throws IOException {
	    try {
	      t.write(protocolOut);
	      bufferedOut.flush();
	    } catch (TException e) {
	      throw new IOException(e);
	    }
	  }
	  
	  /**
	    * Close the file stream.
	    */
	  public void close() throws IOException {
	    bufferedOut.close();
	  }

}
