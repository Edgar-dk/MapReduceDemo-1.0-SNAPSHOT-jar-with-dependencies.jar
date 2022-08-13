package com.sias.mapreduce.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-13 10:48
 * @faction:
 */
public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        /*1.可以理解成是Reducer最终的输出类型*/
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\reduceJoin"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\reduceJoin1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
