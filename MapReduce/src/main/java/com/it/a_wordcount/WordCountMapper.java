package com.it.a_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-13 12:33
 * @description
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);


    /**
     * 每一行数据即每一个<K, V>都会单独调用这个map方法
     * @param key  这一行的键
     * @param value 这一行的值
     * @param context 上下文环境，连接MapReduce和系统交互的桥梁，并行传递消息
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1、获取一行
        String line = value.toString();

        // 2、切割
        String[] words = line.split(" ");

        // 3、循环写出
        for (String word : words) {

            outK.set(word);

            // 写出
            context.write(outK, outV);
        }
    }
}
