package com.sias.mapreduce.partitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-04 20:56
 * @faction:
 */
public class FlowBean implements Writable {
    /*1.定义变量保存数据*/
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    public FlowBean() {
    }
    /*2.定义get set设置数据*/
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
    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }
    /*01.序列化方法
    *    数据出去的排列方式
    *    write方式写出去*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }
    /*02.反序列化方法
    *    read的方式读进来
    *    也是按照这种规则读数据*/
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }
    @Override
    public String toString() {
        return upFlow +"\t"+ downFlow +"\t"+ sumFlow ;
    }


}
