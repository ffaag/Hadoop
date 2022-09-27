package com.it.k_etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-20 10:59
 * @description
 */
public class WebLogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WebLogDriver.class);
        job.setMapperClass(WebLogMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("F:\\0Data\\input\\input08"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\0Data\\output\\output11"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);


    }

}
