package com.hadoopsort.algorithm;

import com.hadoopsort.models.UniformData;
import com.hadoopsort.tools.Constants;
import com.hadoopsort.tools.Logger;
import com.hadoopsort.tools.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * Date: 11/29/12
 * Time: 1:54 PM
 * Author: spirosoikonomakis
 * To change this template use File | Settings | File Templates.
 */
public class MapReducing extends Configured implements Tool {

	private static final Log _log = LogFactory.getLog(MapReducing.class);
	private static Logger LOGGER;

	private static final String INPUT = "/user/soikonomakis/datatest/Data_2_10000.txt";
	private static final String OUTPUT = "/user/soikonomakis/data-out/";
	
    public static List<UniformData> topKRecords =null;
	private Random random = new Random();
	public static int k=0;
	public static String resultsFilename = "";

	public static void main(String[] args)throws Exception{
		k=Integer.valueOf(args[0]);
		int ret = ToolRunner.run(new MapReducing(), args);
		
		LOGGER = new Logger(Constants.logs);
		LOGGER.write("Job-1 for Mapper-1 (Sorting Data)", 
				"Simulation Time",smlTimes.get(0), null);

		LOGGER.write("Job-2 for Mapper-2 (Collect Top-"+k+")", 
				"Simulation Time",smlTimes.get(1), null);
		
		ArrayList<UniformData> topKRecords = Utils.readFromLocalFile(Constants.resultsDir, resultsFilename);
		LOGGER.write("---------TOP-K RECORDS RESULTS---------", 
				"---------------------------------------", 
				null, 
				topKRecords);

		System.exit(ret);

	}
	@Override
	public int run(String[] strings) throws Exception {
		Configuration conf = getConf();
		conf.setInt("krecords", k);
        conf.set("fs.default.name", "hdfs://127.0.0.1:54310/");
        FileSystem dfs = FileSystem.get(conf);

		Job job1 = Utils.setJob(_log,conf,MapReducing.class,
				DoubleWritable.class,
				UniformData.class,
				Text.class,
				Text.class, 
				TextInputFormat.class,
				TextOutputFormat.class,
				FirstMapper.class,
				Reducer.class,"|--Sorting of the data tuples--|");

		Job job2= Utils.setJob(_log,conf,
				MapReducing.class,
				DoubleWritable.class,
				UniformData.class,
				Text.class,
				Text.class, 
				TextInputFormat.class,
				TextOutputFormat.class,
				SecondMapper.class,
				Reducer.class,"|--Scan the first k tuples only and report them as result--|");
		
		String filename = Utils.getDateFormatted()+"_"+random.nextInt()+".txt";
		String output = OUTPUT+ filename;
		Utils.setInputOutputPaths(_log,job1, new Path[] { new Path(INPUT) },new Path(output));
		
		filename = Utils.getDateFormatted()+".txt";
		Utils.setInputOutputPaths(_log,job2, new Path[] { new Path(output) },new Path(OUTPUT+filename ));
		
		boolean success = runJob(conf, job1);
		if(success){
			success = runJob(conf, job2);
			job2.getWorkingDirectory();
		    FileSystem hdfsFileSystem = FileSystem.get(conf);
		    if (hdfsFileSystem.exists(new Path(output))){
		    	dfs.copyToLocalFile(new Path(dfs.getWorkingDirectory()+"/data-out/"+filename),
		    			new Path(Constants.resultsDir));
		    	resultsFilename = filename;
		    }

		}
		
		return success ? 0 :1;
	}
	
	private Integer counter=1;
	private static ArrayList<Long> smlTimes= new ArrayList<Long>();
	
	/**
	 * 
	 * @param conf hadoop configuration
	 * @param job each job for map reduce
	 * @return success if job runs properly
	 */
	private boolean runJob(Configuration conf,Job job){
		long lStart = 0,lEnd=0;
		boolean success = false;
		try {
			lStart = System.currentTimeMillis();
			success = job.waitForCompletion(true);
			lEnd = System.currentTimeMillis();
			smlTimes.add(lEnd -lStart);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		counter++;
			return success;

		}
	public static List<UniformData> getTopKRecords() {
		return topKRecords;
	}
	public static void setTopKRecords(List<UniformData> topKRecords) {
		MapReducing.topKRecords = topKRecords;
	}
	
}
