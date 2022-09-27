package com.it.e_partitioner2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-14 10:19
 * @description
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 1、获取一行
        String line = value.toString();

        // 2、切割
        String[] split = line.split("\t");

        // 3、抓取想要的数据
        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];

        // 4、封装
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();

        // 5、写入
        context.write(outK,outV);

    }
}
