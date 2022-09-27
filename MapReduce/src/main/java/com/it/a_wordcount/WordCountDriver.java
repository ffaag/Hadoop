package com.it.a_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-13 12:33
 * @description
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1、获取job
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        // 2、设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3、关联Mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、设置Map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\0Data\\input\\input1"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\0Data\\output\\output1"));

        // 7、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }


}
