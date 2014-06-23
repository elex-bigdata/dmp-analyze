package com.elex.dmp.analyze;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class UserCountByType extends Configured implements Tool {

	public static class MyMapper extends TableMapper<Text, IntWritable> {
		
		private String uid;
		private IntWritable one = new IntWritable(0);
		

		@Override
		protected void map(ImmutableBytesWritable key, Result values,Context context) throws IOException, InterruptedException {
			uid = Bytes.toString(getUid(key));	
			if(!values.isEmpty()){
				byte type = Bytes.head(key.get(), 1)[0];
				switch(type){
				case 0:
					context.write(new Text(uid+"-0"), one);
					break;
				case 1:
					context.write(new Text(uid+"-1"), one);
					break;
				case 3:
					context.write(new Text(uid+"-2"), one);
				}
				
				
			}

			
		}

		private byte[] getUid(ImmutableBytesWritable key) {

			return Bytes.tail(key.get(), key.get().length-9);
		}

	}

	
	
	 
	public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {

		private int count;
		private String[] kv;

		
		@Override
		protected void reduce(Text uid, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
			count = 0;
			kv = uid.toString().split("-");
			for(IntWritable t:value){				
				count++;
			}
			context.write(null, new Text(kv[0]+","+kv[1]+","+Integer.toString(count)));
			
		}
		
	}
	
	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 int res = ToolRunner.run(new Configuration(), new UserCountByType(), otherArgs);
		 System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
        conf = HBaseConfiguration.create(conf);
        Job job = Job.getInstance(conf,"user-count-bytype");
        job.setJarByClass(UserCountByType.class);   
        byte[] type =new byte[]{Byte.parseByte(args[0])};
        long now =System.currentTimeMillis();
        long before = now - Long.parseLong(args[1])*60*1000;
        byte[] startRow = Bytes.add(type,Bytes.toBytes(before));
        byte[] stopRow = Bytes.add(type,Bytes.toBytes(now));       
		Scan s = new Scan();
		s.setStartRow(startRow);
		s.setStopRow(stopRow);
		s.setCaching(500);
		s.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("url"));
		TableMapReduceUtil.initTableMapperJob("dmp_user_action", s, MyMapper.class,Text.class, IntWritable.class, job);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true)?0:1;
	}

}
