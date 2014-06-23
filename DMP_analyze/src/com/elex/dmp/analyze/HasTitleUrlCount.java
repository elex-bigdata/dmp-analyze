package com.elex.dmp.analyze;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HasTitleUrlCount extends Configured implements Tool {

	public static class MyMapper extends TableMapper<Text, Text> {
		
		private Text uid = new Text();
		private Text url = new Text();
		

		@Override
		protected void map(ImmutableBytesWritable key, Result values,Context context) throws IOException, InterruptedException {
			uid.set(getUid(key));	
			for (KeyValue kv : values.raw()) {
				if ("ua".equals(Bytes.toString(kv.getFamily())) && "url".equals(Bytes.toString(kv.getQualifier()))) {
					url.set(kv.getValue());					
					context.write(uid, url);
				}
			}

			
		}

		private byte[] getUid(ImmutableBytesWritable key) {

			return Bytes.tail(key.get(), key.get().length-9);
		}

	}

	
	
	 
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

				

		private HTable ud;
		private Configuration configuration;		
		private int countHasTitle,countAll;
		private BloomFilter f =new BloomFilter();
		private Element ele;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			configuration = HBaseConfiguration.create();
			ud = new HTable(configuration, "dmp_url_detail");
			Scan s = new Scan();
			s.setCaching(1000);
			s.addFamily(Bytes.toBytes("ud"));
			ResultScanner rs = ud.getScanner(s);
			for (Result r : rs) {
				if(!r.isEmpty()){
					for(KeyValue kv:r.raw()){
						if ("ud".equals(Bytes.toString(kv.getFamily())) && "k".equals(Bytes.toString(kv.getQualifier()))) {	
							ele = new Element(Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-1)));
							f.add(ele);
						}
					}											
				}
			}
				
			
			
		}

		@Override
		protected void reduce(Text uid, Iterable<Text> urlList,Context context) throws IOException, InterruptedException {
			countHasTitle = 0;
			countAll = 0;
			for(Text key:urlList){
				countAll++;
				ele = new Element(key.toString());
				if(f.exist(ele)){
					countHasTitle++;
				}
			}								
									
			context.write(null, new Text(uid+","+Integer.toString(countAll)+","+Integer.toString(countHasTitle)));
		}
		
	}
	
	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 int res = ToolRunner.run(new Configuration(), new HasTitleUrlCount(), otherArgs);
		 System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
        conf = HBaseConfiguration.create(conf);
        Job job = Job.getInstance(conf,"haskeycount");
        job.setJarByClass(HasTitleUrlCount.class);   		
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
		TableMapReduceUtil.initTableMapperJob("dmp_user_action", s, MyMapper.class,Text.class, Text.class, job);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true)?0:1;
	}

}
