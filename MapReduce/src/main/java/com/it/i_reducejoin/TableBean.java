package com.it.i_reducejoin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-11-19 15:38
 * @description
 */

@Data
@AllArgsConstructor
@NoArgsConstructor

public class TableBean implements Writable, WritableComparable<TableBean> {

    private String id;    // 订单id
    private String pid;   // 商品id
    private int amount;   // 商品数量
    private String pname; // 商品名称
    private String flag;  // 文件名，标记是哪张表


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.flag = in.readUTF();
    }

    @Override
    public int compareTo(TableBean o) {
        if(Integer.parseInt(this.id) > Integer.parseInt(o.id)){
            return 1;
        }else if(Integer.parseInt(this.id) < Integer.parseInt(o.id)){
            return -1;
        }else {
            return 0;
        }
    }
}
