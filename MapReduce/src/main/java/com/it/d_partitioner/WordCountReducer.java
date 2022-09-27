package com.it.d_partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-13 12:33
 * @description
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();


    /**
     * 每一种key都会调用这个方法，即同一种单词调用一次来统计数量
     *
     * @param key     切割后的键，是一个单词
     * @param values  这个单词的所有数量，如：(1，1，1，1)这种，只是把1合起来，并没有累加
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        context.write(key, outV);
    }
}
