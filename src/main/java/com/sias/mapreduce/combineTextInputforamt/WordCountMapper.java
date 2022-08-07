package com.sias.mapreduce.combineTextInputforamt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-07-20 9:33
 * @faction:
 */
public class WordCountMapper extends Mapper<LongWritable,Text,  Text,IntWritable> {

    private Text outk=new Text();
    private IntWritable outv=new IntWritable(1);


    /*1.这个是输入的*/
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*01.把输入的数据转成字符串形
        *    一行的数据可能有很多，这个时候
        *    可以对数据进行切割，切割的时候，需要
        *    按照某种规则切割*/
        String line = value.toString();

        /*02.切割*/
        String[] words = line.split(" ");

        /*03.循环写出*/
        for (String word : words) {
            /*001.封装Word，数组中有很多Word所以需要
            *     循环的方式*/
            outk.set(word);
            /*002.写出*/
            context.write(outk,outv);
        }


    }
}
