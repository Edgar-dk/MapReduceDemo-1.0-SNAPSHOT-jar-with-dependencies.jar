package com.sias.mapreduce.combineTextInputforamt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-07-20 9:34
 * @faction:
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*01.获取配置信息以及获取Job对象*/
        Configuration configuration = new Configuration();
        Job instance = Job.getInstance();
        /*02.关联本Driver程序的Jar*/
        instance.setJarByClass(WordCountDriver.class);
        /*03.关联本Mapper和Reduce*/
        instance.setMapperClass(WordCountMapper.class);
        instance.setReducerClass(WordCountReduce.class);
        /*04.设置Mapper输出的kv类型*/
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
        /*05.设置最终输出kv类型*/
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);

        instance.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(instance,4194304);

        /*06.设置输入和输出路径
        *    输入的路径里面的文件，表示计算这个文件
        *    输出的文件，表示计算完成存放文件的位置*/
        FileInputFormat.setInputPaths(instance,new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\inputCombinTextforma"));
        FileOutputFormat.setOutputPath(instance,new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\outputCombineText1"));
        /*07.提交job*/
        boolean forCompletion = instance.waitForCompletion(true);
        System.exit(forCompletion ? 0 :1);
    }
}
