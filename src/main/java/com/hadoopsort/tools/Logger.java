package com.hadoopsort.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.hadoopsort.models.UniformData;

public class Logger {
	public File file;

	public Logger() {
		super();
	}
	public Logger(File file) {
		super();
		this.file = file;
		try {
			if(!file.exists())
				file.createNewFile();
			else
				file.delete();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @param msg title of string
	 * @param status 
	 * @param time simulation time
	 * @param results for topK records
	 */
	public void write(String msg,
			String status,Long time, List<UniformData> results){

		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(file.getName(), true));
			writer.append(msg);
			writer.newLine();
			
			if (time!=null)
				writer.append(status+"="+time+"msec");
			else
				writer.append(status);
			
			writer.newLine();
			writer.newLine();
			if(results!=null){
				for(UniformData str: results) {
					writer.append("ID="+str.getId()+" Score = "+str.getScore());
					writer.newLine();
				}
			}
			writer.close();	
		} catch (IOException e) {
			e.printStackTrace();
		}	

	}
	public File getFile() {
		return file;
	}
	public void setFile(File file) {
		this.file = file;
	}
	


}
