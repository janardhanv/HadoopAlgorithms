package com.hadoopsort.comparators;

import com.hadoopsort.models.UniformData;

import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: soikonomakis
 * Date: 1/8/13
 * Time: 9:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class UniformDataComparator implements Comparator<UniformData> {
    @Override
    public int compare(UniformData uniformData, UniformData uniformData1) {
        if(uniformData.getScore()<uniformData1.getScore())
            return 1;
        else
            return -1;
    }
}
