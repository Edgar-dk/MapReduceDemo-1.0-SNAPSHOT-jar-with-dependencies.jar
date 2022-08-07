package com.sias.mapreduce.WordCount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-07-20 9:33
 * @faction:
 */

/*一：第一个是Map输入过来的数据
*     数据的形式是String，int类型的，对应到文本
*     是Text , IntWritable，类型的，过来的数据是
*     可能是重复的，Reducer目的就是为了去统计这些重复的
*     看看这些重复的有多少，最后在输出*/
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable outV = new IntWritable();

    /*1.reduce目的就是为了统计一个单词的个数
    *   Text  key是过来的sias这个单词，Text，表示
    *   sias是一个String类型的，key是这个单词本身，
    *   后面的参数，表示这个数的类型，以及数的值values
    *   是很多这样sias单词的数*/
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        /*01.当过来一个sias,1 这样形式的数据，
        *    数据存在values中，先去遍历values里面的数据
        *    一个的话，把sum+1，value.get执行+1的操作
        *    最后统计完数据之后把数据放在这样类型中，然后输出
        *    key还是过来的那个sias，原封不动的放在哪里，一并输出*/
        for (IntWritable value : values) {
            sum+=value.get();
        }

        outV.set(sum);

        context.write(key,outV);
    }
}
