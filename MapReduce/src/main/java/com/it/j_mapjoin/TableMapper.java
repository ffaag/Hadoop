package com.it.j_mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author ZuYingFang
 * @time 2021-11-19 15:50
 * @description
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text outK = new Text();
    private HashMap<String, String> stringStringHashMap = new HashMap<>();


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 获取缓存数据，并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFiles[0]));

        // 从流中读取数据
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, "UTF-8"));
        String line;

        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {  // 一行一行的内容
            // 切割
            String[] split = line.split("\t");

            stringStringHashMap.put(split[0], split[1]);

        }

        // 关流
        IOUtils.closeStream(bufferedReader);

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");

        String pname = stringStringHashMap.get(split[1]);

        // 获取订单Id和订单数量
        outK.set(split[0] + "\t" + pname + "\t" + split[2]);

        context.write(outK, NullWritable.get());

    }
}
