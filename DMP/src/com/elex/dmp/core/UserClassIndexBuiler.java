package com.elex.dmp.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.elex.dmp.utils.PropertiesUtil;

public class UserClassIndexBuiler extends Configured implements Tool {
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "IndexBuilder");
		job.setJarByClass(UserClassIndexBuiler.class);
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("dmp_user_classify", null, job);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class IndexMapper extends Mapper<Text, VectorWritable, ImmutableBytesWritable, Put> {
		

		


		private long checkpoint = 1000;
		private long count = 0;
		private DecimalFormat df = new DecimalFormat("0.000000");
		private StringBuffer str = new StringBuffer();
		private Iterator<Element> it;
		private Element e;
		private Map<Integer,Integer> dmpList = new HashMap<Integer,Integer>();
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf); 
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(PropertiesUtil.getModelFilePath()))));
			String line = null;
			int i = 0;
			while (i < Integer.parseInt(PropertiesUtil.getNumTopics()) && (line = br.readLine()) != null) {
				try {
					JSONObject jsonObject = new JSONObject(line);
					JSONArray dspList = jsonObject.getJSONArray("dspMap");
					
					dmpList.put(jsonObject.getInt("id"),Integer.parseInt(dspList.getJSONObject(0).keys().next().toString()));
				} catch (JSONException e) {
					e.printStackTrace();
					System.err.println(line);
				}

				i++;
			}
			br.close();

		}
		
		@Override
		public void map(Text key, VectorWritable line, Context context)throws IOException {

			// Extract each value
			byte[] row = Bytes.toBytes(key.toString());
			byte[] family = Bytes.toBytes("uc");
			byte[] dt = Bytes.toBytes("dt");// doc-topic
			byte[] m = Bytes.toBytes("M");			
			
			it = line.get().iterator();
			str.delete(0, str.length());

			while (it.hasNext()) {
				e = it.next();
				str.append(e.index()).append(":").append(df.format(e.get())).append(",");				
			}

			byte[] dtv = Bytes.toBytes(str.toString().substring(0, str.toString().length()-1));
			byte[] mv = Bytes.toBytes(Integer.toString(dmpList.get(line.get().maxValueIndex())));
			
			
			
			// Create Put
			Put put = new Put(row);
			put.add(family, dt, dtv);
			put.add(family, m, mv);

			
			
			put.setWriteToWAL(false);

			try {
				context.write(new ImmutableBytesWritable(row), put);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// Set status every checkpoint lines
			if (++count % checkpoint == 0) {
				context.setStatus("Emitting Put " + count);
			}
		}
	}
}
