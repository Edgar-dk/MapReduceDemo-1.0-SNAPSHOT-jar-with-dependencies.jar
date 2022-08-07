package com.sias.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-05 11:32
 * @faction:
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*01.获取job
        *    即可理解为configuration就是加载hadoop中的配置信息。
        *    先加载Hadoop里面的配置信息，然后，使用Hadoop
        *    里面的配置文件，把数据数据到指定的位置（其中一种方式
        *    还有另外一种方式）*/
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        /*02.配置Jar*/
        job.setJarByClass(FlowDriver.class);

        /*03.关联Mapper和Reducer*/
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        /*04.设置mapper输出的key和value类型*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        /*05.设置最终输出的key和value类型*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /*06.设置输入和输出路径*/
        FileInputFormat.setInputPaths(job, new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\inputFFlow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\out1"));

        /*07.提交Job*/
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }

}
