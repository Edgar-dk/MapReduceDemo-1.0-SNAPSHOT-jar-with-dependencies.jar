package com.sias.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-05 7:51
 * @faction:
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    /*1.LongWritable, Text
     *   是直接把数据传递给Map的形式，
     *   然后Map输出数据在按照Text,FlowBean的形式
     *   传递给Reducer，数据传递给Reduce需要把数据
     *   放在对象中，传递，所以创建了下面两个对象放数据*/
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        /*01.获取一行信息*/
        String line = value.toString();
        /*02.把一行内容按照\t空格去切割*/
        String[] split = line.split("\t");
        /*03.抓去想要的数据
         *    倒叙的方式去采取数据*/
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        /*04.封装*/
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();
        /*05.写出
         *    Reduce接收数据*/
        context.write(outK, outV);

    }
}
