package com.it.partitionerAndWritableCompareable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author ZuYingFang
 * @time 2021-11-18 11:04
 * @description
 */
public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        String phone = text.toString();
        String substring = phone.substring(0, 3);
        int partition;

        if ("136".equals(substring)) {
            partition = 0;
        } else if ("137".equals(substring)) {
            partition = 1;
        } else if ("138".equals(substring)) {
            partition = 2;
        } else if ("139".equals(substring)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
