package edu.uchicago.mpcs53013;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

public class DemoHadoopThriftSerialization {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			String serializations = conf.get("io.serializations").trim();
			String delim = serializations.isEmpty() ? "" : ",";
			conf.set("io.serializations", serializations + delim + StudentSerialization.class.getName());
			FileSystem fs = FileSystem.get(conf);
			Path seqFilePath = new Path("/tmp/thrift.out");
			SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(seqFilePath),
					SequenceFile.Writer.keyClass(Student.class), SequenceFile.Writer.valueClass(IntWritable.class),
					SequenceFile.Writer.compression(CompressionType.NONE));
			
			Student student = new Student("Mike", 25, StudentType.MPCS);
			writer.append(student, new IntWritable(1));
			student.setName("Debby");
			student.setHomeworkTotal(65);
			student.setType(StudentType.COLLEGE);
			writer.append(student, new IntWritable(2));
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
