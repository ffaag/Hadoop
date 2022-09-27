package com.it.g_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-18 11:26
 * @description 可有可无，就是在每个maptask完成之后就预先的汇总，因此要继承reducer类，重写相应的方法，同时里面的代码也是和reducer类一样
 * 另外，reducer是将所有的maptask进行汇总，combine预先把一个maptask的数据进行汇总就可以减轻传输压力
 * 既然combine和reducer的代码一模一样，我们就不用自己再写一个combiner了，只要前后的业务一样，可以使用combiner，可以直接将combiner类设置为reducer类job.setCombinerClass(WordCountReducer.class);
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key, outV);
    }
}
