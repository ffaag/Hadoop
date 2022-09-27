package com.it.j_mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author ZuYingFang
 * @time 2021-11-19 15:50
 * @description
 */
public class TableDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        // 加载缓存数据
        job.addCacheFile(new URI("file:///F:/0Data/input/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("F:\\0Data\\input\\input07"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\0Data\\output\\output10"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
