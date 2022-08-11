package com.sias.mapreduce.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean,Text, Text, FlowBean> {

    /*1.数据虽然是按照手机号，FlowBean的形式进来的，在
    *   处理这些数据的时候，需要按照FlowBean做为key，
    *   手机号作为value，计算得出结果后，在按照手机号，FlowBean的
    *   形式输出数据*/
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
