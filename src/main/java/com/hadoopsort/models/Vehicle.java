package com.hadoopsort.models;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Vehicle implements WritableComparable<Vehicle>{
	private String brand;
	private String model;
	private Long mileage;
	
	public Vehicle() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Vehicle(String brand, String model, Long mileage) {
		super();
		this.brand = brand;
		this.model = model;
		this.mileage = mileage;
	}
	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(mileage)
				.append(',')
				.append(model)
				.append('}')
				.toString();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		model =WritableUtils.readString(in);
		mileage = in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, model);
		out.writeLong(mileage);
		
	}
	@Override
	public int compareTo(Vehicle veh) {
		int result = mileage.compareTo(veh.mileage);
		return result;
	}
	
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getModel() {
		return model;
	}
	public void setModel(String model) {
		this.model = model;
	}
	public Long getMileage() {
		return mileage;
	}
	public void setMileage(Long mileage) {
		this.mileage = mileage;
	}
	
}
