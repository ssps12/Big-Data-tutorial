package edu.uchicago.mpcs53013;
import java.io.File;

public class DemoSerialization {

	public static void main(String[] args) {
		ThriftWriter thriftOut = new ThriftWriter(new File("/tmp/thrift.out"));
		try {
			// Open writer
			thriftOut.open();
	
			Student student = new Student("Mike", 25, StudentType.MPCS);
			thriftOut.write(student);
			student.setName("Debby");
			student.setHomeworkTotal(65);
			student.setType(StudentType.COLLEGE);
			thriftOut.write(student);
			// Close the writer
			thriftOut.close();
		} catch (Exception e) {
			
		}
	}

}
