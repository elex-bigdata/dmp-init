package com.elex.dmp.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class TextFileToSequceFileConvertor {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		convert(new Path(args[0]),new Path(args[1]));

	}

	public static void convert(Path src, Path dist) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		SequenceFile.Reader reader = null;
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(dist), SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class));

		Path hdfs_src;
		FileStatus[] srcFiles = fs.listStatus(src);
		Text key = new Text();
		Text value = new Text();
		
		for (FileStatus file : srcFiles) {

			if (!file.isDirectory()) {
				hdfs_src = file.getPath();
				if (file.getPath().getName().contains("part")) {
					try {
						reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(hdfs_src));
						while (reader.next(key, value)) {
							writer.append(key, value);
						}

					} finally {
						IOUtils.closeStream(reader);
					}
				}
			}
		}
		
		writer.close();

	}

}
