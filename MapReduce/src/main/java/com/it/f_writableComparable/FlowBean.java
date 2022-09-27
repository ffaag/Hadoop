package com.it.f_writableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-14 10:05
 * @description 序列化
 * 1、实现writable接口
 * 2、重写序列化和反序列化方法
 * 3、重写一个空参构造方法
 * 4、重写toString方法
 */
public class FlowBean implements Writable, WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    // 重写空参构造方法
    public FlowBean() {
    }


    // 序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }


    // 反序列化方法，反序列的顺序必须和序列化的顺序一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }


    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }


    @Override
    public int compareTo(FlowBean o) {

        // 总流量倒叙排序
        if (this.sumFlow > o.sumFlow) {
            return -1;  // 倒叙返回-1，正序返回1
        } else if (this.sumFlow < o.sumFlow) {
            return 1;
        } else {
            // 二次排序，当总流量一样时，按照上行流量正序排
            if (this.upFlow > o.upFlow) {
                return 1;  // 正序
            } else if (this.upFlow < o.upFlow) {
                return -1;
            } else {
                return 0;
            }
        }

    }
}
