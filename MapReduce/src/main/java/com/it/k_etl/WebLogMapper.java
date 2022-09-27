package com.it.k_etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-20 10:53
 * @description
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        boolean result = parseLog(line, context);

        if(!result){
            return;
        }

        context.write(value, NullWritable.get());

    }

    private boolean parseLog(String line, Context context) {
        String[] s = line.split(" ");
        if (s.length>11) {
            return true;
        }
        return false;
    }
}
