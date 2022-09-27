package com.it.d_partitioner;

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
 * @description shuffer在 map之后，但他占据了一部分的reducer，所以并不是夹在两者的中间
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


        // 设置reducemask个数，设置之后reducemask和分区相同个数，如果自行设置分区也设置reducemsk个数，则就可以不相等了
        // 有几个reducemask就有几个输出结果集，如果分区数小于reducemask，那么有的结果集就是空的
        job.setNumReduceTasks(2);   


        // 6、设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\0Data\\input\\input03"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\0Data\\output\\output04"));

        // 7、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }


}
