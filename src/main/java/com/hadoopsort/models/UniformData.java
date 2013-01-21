package com.hadoopsort.models;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: soikonomakis
 * Date: 1/8/13
 * Time: 9:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class UniformData implements WritableComparable<UniformData> {
    private Long id;
    private Double score;
    public UniformData() {
		super();
	}
    public UniformData(Long id, Double score) {
        this.id = id;
        this.score = score;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
    @Override
    public String toString() {
        return (new StringBuilder())
                .append(',')
                .append(score)
                .append(',')
                .append(id)
                .toString();
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        id = WritableUtils.readVLong(in);
        score = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, id);
        out.writeDouble(score);

    }
    @Override
    public int compareTo(UniformData uniformData) {
        int result = score.compareTo(uniformData.score);
        return result;
    }
}
