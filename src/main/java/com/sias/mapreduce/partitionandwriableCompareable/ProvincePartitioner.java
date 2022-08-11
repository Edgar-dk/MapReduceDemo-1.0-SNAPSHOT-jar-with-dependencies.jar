package com.sias.mapreduce.partitionandwriableCompareable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Edgar
 * @create 2022-08-10 15:03
 * @faction:
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();
        String substring = phone.substring(0, 3);
        int partitioner;
        if ("136".equals(substring)){
            partitioner = 0;
        }else if ("137".equals(substring)){
            partitioner = 1;
        }else if ("138".equals(substring)){
            partitioner = 2;
        }else if ("139".equals(substring)){
            partitioner = 3;
        }else {
            partitioner = 4;
        }
        return partitioner;
    }
}
