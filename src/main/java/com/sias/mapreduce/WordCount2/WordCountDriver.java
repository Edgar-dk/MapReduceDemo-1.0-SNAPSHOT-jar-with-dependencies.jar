package com.sias.mapreduce.WordCount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
        /*06.设置输入和输出路径
        *    输入的路径里面的文件，表示计算这个文件
        *    输出的文件，表示计算完成存放文件的位置
        *    这个是在本地环境下的运行，到虚拟机上运行
        *    的话，需要动态的传递参数，0表示传递第一个参数
        *    1传递的第二个参数*/
        FileInputFormat.setInputPaths(instance,new Path(args[0]));
        FileOutputFormat.setOutputPath(instance,new Path(args[1]));
        /*07.提交job*/
        boolean forCompletion = instance.waitForCompletion(true);
        System.exit(forCompletion ? 0 :1);
    }
}
