package com.sias.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-10 16:03
 * @faction:
 */

/*一：进来的是<a,1>这种类型的数据，出去的时候，也是这中类型的数据*/
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        /*01：进来的数据都是按照<a,1><a,1><b,1>字母顺序排序的，当<a,1>进来之后
        *     先去循环<a,1>把很多个<a,1>循环结束之后，在循环体中，value就是数字1
        *     把很多个1相加起来之后，在去处理<b,1>这种类型数据，在把value数据相加起来
        *     直到数据处理完结束*/
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key,outV);
    }
}
