package com.it.h_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-18 21:16
 * @description
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {


    private FSDataOutputStream out2;
    private FSDataOutputStream out1;

    public LogRecordWriter(TaskAttemptContext job) {
        // 创建两条流
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            out1 = fileSystem.create(new Path("F:\\0Data\\output\\output07\\out1"));
            out2 = fileSystem.create(new Path("F:\\0Data\\output\\output08\\out2"));


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        // 具体写
        String log = key.toString();

        if (log.contains("atguigu")) {
            out1.writeBytes(log + "\n");
        } else
            out2.writeBytes(log + "\n");
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(out1);
        IOUtils.closeStream(out2);
    }
}
