package com.elex.dmp.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

		BufferedReader reader = null;
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(dist), SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class));

		Path hdfs_src;
		FileStatus[] srcFiles = fs.listStatus(src);
		String line;
		
		for (FileStatus file : srcFiles) {

			if (!file.isDirectory()) {
				hdfs_src = file.getPath();
				if (file.getPath().getName().startsWith("0")) {
					try {
						reader =new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));
						line=reader.readLine();
						while (line != null) {
							if(line.split(",").length==2){
								writer.append(new Text(line.split(",")[0]), new Text(line.split(",")[0]));
							}						
							line = reader.readLine();
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
