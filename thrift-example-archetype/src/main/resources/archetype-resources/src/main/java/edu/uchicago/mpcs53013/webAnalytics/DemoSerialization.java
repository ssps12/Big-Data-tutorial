package edu.uchicago.mpcs53013.webAnalytics;
import java.io.File;

public class DemoSerialization {

	public static void main(String[] args) {
		ThriftWriter thriftOut = new ThriftWriter(new File("/tmp/trish.thrift.out"));
		try {
			// Open writer
			thriftOut.open();
	
			PageID pageID = new PageID();
			pageID.setUrl("http://www.zaphoyd.com");
			thriftOut.write(pageID);
			pageID.setUrl("http://www.trish.com");
			thriftOut.write(pageID);
			// Close the writer
			thriftOut.close();
		} catch (Exception e) {
			
		}
	}

}
