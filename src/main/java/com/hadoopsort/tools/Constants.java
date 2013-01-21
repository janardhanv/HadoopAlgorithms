package com.hadoopsort.tools;

import java.io.File;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * Date: 11/29/12
 * Time: 2:51 PM
 * Author: spirosoikonomakis
 * To change this template use File | Settings | File Templates.
 */
public class Constants {
    public static final SimpleDateFormat sdf = new SimpleDateFormat("HH-mm-ss");
    public static final File logs = new File("logger.txt");
    public static final String hadoopNamenode = "hdfs://localhost:54310/";
    public static final String resultsDir = "results";
}
