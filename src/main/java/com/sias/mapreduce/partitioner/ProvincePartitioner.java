package com.sias.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Edgar
 * @create 2022-08-07 9:16
 * @faction:
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        /*01.text里面存放的是手机号*/
        String phone = text.toString();

        /*02.截取字符串里面的数据，从0开始，到3结束，包括0（第一个数），不包括3（是第4个数）*/
        String prePhone = phone.substring(0, 3);
        int partitioner;
        if ("136".equals(prePhone)) {
            partitioner = 0;
        } else if ("137".equals(prePhone)) {
            partitioner = 1;
        } else if ("138".equals(prePhone)) {
            partitioner = 2;
        } else if ("139".equals(prePhone)) {
            partitioner = 3;
        } else {
            partitioner = 4;
        }

        return partitioner;
    }
}
