package com.hadoopsort.algorithm;

import com.hadoopsort.models.UniformData;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * Date: 11/29/12
 * Time: 2:25 PM
 * Author: spirosoikonomakis
 * To change this template use File | Settings | File Templates.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<DoubleWritable,UniformData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<UniformData> values, Context context) throws IOException, InterruptedException {
        Text k = new Text(key.toString());
        int count = 0;

        Iterator<UniformData> it = values.iterator();
        while(it.hasNext()) {
            Text v = new Text(it.next().toString());
            context.write(k, v);
        }
    }
}

