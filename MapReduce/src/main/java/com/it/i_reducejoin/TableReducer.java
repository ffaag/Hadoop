package com.it.i_reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author ZuYingFang
 * @time 2021-11-19 15:50
 * @description
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if("order".equals(value.getFlag())){
                // hadoop中记录地址，因此不能直接把value丢到list中，必须新创建对象，把value复制给对象再丢进去
                TableBean tableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tableBean, value);   // 把value复制给tableBean
                    orderBeans.add(tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    // 因为以pid作为key，因此不同的pid进入不同的ReduceTask，一个ReduceTask只有一个pdBean
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());

            context.write(orderBean, NullWritable.get());
        }
    }
}
