package com.sias.mapreduce.partitionandwriableCompareable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();


    /*1.指定的文件，会
    *   进来的数据，按照偏移量，文本数据
    *   这个数据会在Mapper中处理，把FlowBean作为key，手机号作为
    *   value，把数据交给Reduce，Reduce，输出数据的时候，按照
    *   key-手机号，value-FlowBean，表明在Mapper和Reduce之间
    *   才是计算数据的地方（就是数据排序的计算时机）*/
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();

        // 切割
        String[] split = line.split("\t");

        // 封装
        outV.set(split[0]);
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();

        // 写出
        context.write(outK, outV);
    }
}
