package com.sias.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-05 11:13
 * @faction:
 */
/*一：Map输出的形式是Text--->手机号
*    FlowBean---->数据对象的形式，Map传递过来的只是，FlowBean的单个形式
*    ，总和需要在Reducer地方结算*/
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    /*1.把这个对象引进来，
    *   存储数据在这个对象中存*/
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        /*01.过来的数据FlowBean存放多个
        *    需要遍历出来，*/
        for (FlowBean value : values) {
            totalUp +=value.getUpFlow();
            totalDown +=value.getDownFlow();
        }
        /*02.把数据放在FlowBean传输出去*/
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        /*03.把数据写出去*/
        context.write(key,outV);

    }
}
