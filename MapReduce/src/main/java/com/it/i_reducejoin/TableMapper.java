package com.it.i_reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-19 15:50
 * @description
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {


    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    // 初始化方法，有两个文件，获取每个文件的名称，一个文件对应一个maptask，因此一个文件执行一次setup方法
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();

        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");
        if (fileName.contains("order")) {

            outK.set(split[1]); // pid

            outV.setId(split[0]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setFlag("order");
            outV.setPname("");
            outV.setPid(split[1]);

        } else {
            outK.set(split[0]);  // pid

            outV.setPname(split[1]);
            outV.setId("");
            outV.setAmount(0);
            outV.setFlag("pd");
            outV.setPid(split[0]);
        }

        context.write(outK, outV);

    }
}
