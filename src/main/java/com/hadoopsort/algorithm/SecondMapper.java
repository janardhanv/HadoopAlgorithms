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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by IntelliJ IDEA.
 * Date: 11/29/12
 * Time: 2:25 PM
 * Author: spirosoikonomakis
 * To change this template use File | Settings | File Templates.
 */
public class SecondMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, DoubleWritable, UniformData>{
    private static final Log _log = LogFactory.getLog(SecondMapper.class);
    private static List<UniformData> _topKRecords =null;
    private Double _max = 0.0;
    private boolean _isFirst=true;
    private int _counter=1;
    private int _k=0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Integer k = Integer.valueOf(conf.get("krecords"));
        //_log.info("Threshold =>" + threshold);
        PriorityQueue<UniformData> sortedQueue = new PriorityQueue<UniformData>(k, new UniformDataComparator());

        String[] tokens = value.toString().split(",");
        Long id = Long.valueOf(tokens[2].trim());
        Double score  = Double.valueOf(tokens[1]);
        if (_isFirst){
            UniformData record = new UniformData(id,score);
            sortedQueue.add(record);
            _max = score;
            _counter++;
            _isFirst = false;
        } else{
            if(_counter<=k){
                score = Utils.calculateScore(tokens);
                if(score>_max){
                    UniformData record = new UniformData(id,score);
                    sortedQueue.add(record);
                    _max = record.getScore();
                    _counter++;
                    //_log.info("Worst Threshold =>" + threshold+" for top-k/counter =>"+counter);
                }
            }else{
                List<UniformData> records = new ArrayList<UniformData>(sortedQueue);
                if(records.size()>0){
                    if(records.get(k-1).getScore()>_max){
                        UniformData record = new UniformData(id,score);
                        sortedQueue.add(record);
                        _max=records.get(k-1).getScore();
                        _counter++;
                        //_log.info("Worst Threshold =>" + threshold+" for after k+1 =>"+counter);
                    }

                }
            }
        }
        Iterator<UniformData> recIter = sortedQueue.iterator();
        Integer kRecords =0;
        while(recIter.hasNext()){
            UniformData record = recIter.next();
            DoubleWritable scoreWR = new DoubleWritable(record.getScore());
            context.write(scoreWR, record);
            if(kRecords==k){
                break;
            }
            kRecords++;
        }
    }    
}
