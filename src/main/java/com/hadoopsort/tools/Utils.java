package com.hadoopsort.tools;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoopsort.models.UniformData;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * Date: 11/29/12
 * Time: 2:09 PM
 * Author: spirosoikonomakis
 * To change this template use File | Settings | File Templates.
 */
public class Utils {
	/**
	 * 
	 * @param _log
	 * @param conf
	 * @param mapReducingCls
	 * @param mapOutKeyCls
	 * @param mapOutValueCls
	 * @param outpKeyCls
	 * @param outpValueCls
	 * @param inputFormatCls
	 * @param outputFormatClass
	 * @param mapperCls
	 * @param reduceCls
	 * @param jobName
	 * @return a job for map/reducing
	 */
	public static Job setJob(Log _log, Configuration conf, Class<?> mapReducingCls,Class<?> mapOutKeyCls,Class<?> mapOutValueCls,
			Class<?> outpKeyCls, Class<?> outpValueCls,Class<? extends InputFormat> inputFormatCls,
			Class<? extends OutputFormat> outputFormatClass,
			Class<? extends Mapper> mapperCls, Class<? extends Reducer> reduceCls,String jobName){

		Job job = null;
		try {
			job = new Job(conf,jobName);

			_log.info("Start creating job with Name:" + job.getJobName() + " \n and ID:" + job.getJobID() + "");

			job.setJarByClass(mapReducingCls);

			job.setMapOutputKeyClass(mapOutKeyCls);
			job.setMapOutputValueClass(mapOutValueCls);

			job.setOutputKeyClass(outpKeyCls);
			job.setOutputValueClass(outpValueCls);

			job.setInputFormatClass(inputFormatCls);
			job.setOutputFormatClass(outputFormatClass);

			job.setMapperClass(mapperCls);
			job.setReducerClass(reduceCls);

			_log.info("SUCCESS job with Name:" + job.getJobName() + " \n and ID:" + job.getJobID() + "");

		} catch (IOException e) {
			e.printStackTrace();
			_log.error("ERROR to create job",e);
		}
		return job;
	}
	/**
	 * 
	 * @param _log
	 * @param job
	 * @param inpPaths
	 * @param outPath
	 * set input/output paths for each job
	 */
	public static void setInputOutputPaths(Log _log, Job job, Path[] inpPaths,Path outPath){

		try {
			_log.info("Start setting input/output paths");

			FileInputFormat.setInputPaths(job, inpPaths);
			FileOutputFormat.setOutputPath(job, outPath);

			_log.info("Success setting input/output ");
		} catch (IOException e) {
			e.printStackTrace();
			_log.error("ERROR to create job",e);
		}
	}
	/**
	 * 
	 * @return date in standard format which is 
	 * declared in Constants class
	 */
	public static String getDateFormatted(){
		Date currentDate = new Date();
		String sDate = Constants.sdf.format(currentDate);
		return sDate;
	}
	/**
	 * 
	 * @param nScores
	 * @return the calculated result of each record
	 */
	public static Double calculateScore(String[] nScores){
		Double score =0.0;

		for(int i=0; i<nScores.length-1; i++){
			score += Double.valueOf(nScores[i+1]);

		}
		return score;
	}
	/**
	 * 
	 * @param conf
	 * @param source
	 * @param dest
	 * @throws IOException
	 */
	public static void addFile(Configuration conf,String source, String dest,
			Path[] logsPaths,String msg,
			String status,Long time, List<UniformData> result) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());

		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// System.out.println("Adding file to " + destination);

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);

		out.close();
		fileSystem.close();
	}
	/**
	 * 
	 * @param conf
	 * @param logsPaths
	 * @param msg
	 * @param status
	 * @param time
	 * @param results
	 */
	public static void writeFile(Configuration conf,Path[] logsPaths,String msg,
			String status,Long time, List<UniformData> results){
		try {
			FileSystem fileSystem = FileSystem.get(conf);

			FSDataOutputStream writer = fileSystem.create(logsPaths[0],false);
			writer.writeUTF(msg);

			if (time!=null)
				writer.writeUTF(status+"="+time+"\\n");
			else
				writer.writeUTF(status);

			if(results!=null){
				writer.writeUTF("--------TOP-K RECORDS RESULTS---------\\n");
				for(UniformData str: results) {
					writer.writeUTF("ID="+str.getId()+" Score = "+str.getScore());
				}
				writer.close();	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @param conf
	 * @param file
	 * @throws IOException
	 */
	public static void readFile(Configuration conf,String file) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		FSDataInputStream in = fileSystem.open(path);

		String filename = file.substring(file.lastIndexOf('/') + 1,
				file.length());

		OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		in.close();
		out.close();
		fileSystem.close();
	}
	/**
	 * 
	 * @param conf
	 * @param file
	 * @throws IOException
	 */
	public static void deleteFile(Configuration conf,String file) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		fileSystem.delete(new Path(file), true);

		fileSystem.close();
	}
	/**
	 * 
	 * @param conf
	 * @param dir
	 * @throws IOException
	 */
	public static void mkdir(Configuration conf,String dir) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(dir);
		if (fileSystem.exists(path)) {
			System.out.println("Dir " + dir + " already not exists");
			return;
		}

		fileSystem.mkdirs(path);

		fileSystem.close();
	}
	/**
	 * 
	 * @param resultsDir
	 * @param filename
	 * @return topKRecords results
	 * @throws IOException
	 */
	public static ArrayList<UniformData> readFromLocalFile(String resultsDir,String filename) 
			throws IOException{
		BufferedReader br;
		ArrayList<UniformData> topKResults = new ArrayList<UniformData>();
		File destDir = new File(resultsDir);
		try {
			for (File file : destDir.listFiles()) {
				if(file.getName().contains("part") && file.isFile() && !file.getName().contains(".part")){
					br = new BufferedReader(new FileReader(resultsDir+"/"+file.getName()));

					try {
						String line = br.readLine();

						while (line != null) {
							String[] result = line.split(",");
							Long id = Long.valueOf(result[2]);
							Double score = Double.valueOf(result[1]);
							UniformData data = new UniformData(id,score);
							topKResults.add(data);
							line = br.readLine();
						}

					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						br.close();
						destDir.delete();
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return topKResults;
	}

}
