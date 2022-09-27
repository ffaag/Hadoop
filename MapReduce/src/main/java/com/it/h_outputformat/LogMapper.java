package com.it.h_outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-18 21:03
 * @description
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {



    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        context.write(value, NullWritable.get());

    }
}
