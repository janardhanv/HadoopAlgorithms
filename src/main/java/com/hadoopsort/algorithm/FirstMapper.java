package com.hadoopsort.algorithm;

import com.hadoopsort.comparators.UniformDataComparator;
import com.hadoopsort.models.UniformData;
import com.hadoopsort.tools.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created with IntelliJ IDEA.
 * User: soikonomakis
 * Date: 1/8/13
 * Time: 10:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class FirstMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, DoubleWritable, UniformData>{
    private static final Log _log = LogFactory.getLog(FirstMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        Long id = Long.valueOf(tokens[0]);
        Double score = Utils.calculateScore(tokens);

        UniformData record = new UniformData(id,score);
        DoubleWritable scoreWR = new DoubleWritable(record.getScore());
        context.write(scoreWR, record);
    }
}
