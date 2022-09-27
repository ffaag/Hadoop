package com.it.e_partitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-14 11:10
 * @description
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        // 设置reducemask个数，设置之后reducemask和分区相同个数，如果自行设置分区也设置reducemsk个数，则就可以不相等了
        // 有几个reducemask就有几个输出结果集，如果分区数小于reducemask，那么有的结果集就是空的
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);


        FileInputFormat.setInputPaths(job, new Path("F:\\0Data\\input\\input02"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\0Data\\output\\output04"));

        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
