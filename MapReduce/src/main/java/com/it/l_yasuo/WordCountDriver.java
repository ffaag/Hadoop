package com.it.l_yasuo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
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

        // 开启map端输出压缩
        entries.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        entries.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

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
        FileInputFormat.setInputPaths(job, new Path("F:\\0Data\\input\\input01"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\0Data\\output\\output01"));


        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        // 7、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }


}
