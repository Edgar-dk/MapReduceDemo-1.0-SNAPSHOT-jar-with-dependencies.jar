package com.sias.mapreduce.writableComparable1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    /*1. 在
     *   处理这些数据的时候，需要按照FlowBean做为key，
     *   手机号作为value，计算得出结果后，在按照手机号，FlowBean的
     *   形式输出数据，进来的数据按照手机号，FlowBean，出去还是
     *   按照这种，但是中间的计算方式不是这种*/
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value,key);
        }
    }
}
