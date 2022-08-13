package com.sias.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-13 8:50
 * @faction:
 */
public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();
    /*1.默认情况下
    *   一个文件，一个切片，一个切片，执行一次MapTask
    *   可以从切片的地方获取一个文件的基本信息，因为一个文件
    *   对应这个一个切片，得到了切片就得到了文件的信息，因为
    *   文件进入到了切片中
    *
    *   注意：一个文件进入到setup，就获取了文件的名字，在其他的方法就不需要获取了*/
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /*01.从context上下文中
             获取切片信息*/
        FileSplit split = (FileSplit) context.getInputSplit();

        /*02.从切片上获取文件的名字
        *    提升到全局变量*/
        fileName = split.getPath().getName();
    }


    /*2.一个文件里面的数据要一行一行的进入map方法
    *   然后数据存储在value中
    *
    *   在指定的文件夹中，有多个文件，先一个一个文件进入
    *   第一个文件进入TableMapper后，获取文件名字之后，
    *   在去获取文件中的一行一行的数据，最终把数据封装在了TableBean中
    *   以一种数据形式输出出去，中间经过了shuffle，然后在
    *   进入Reducer阶段，去处理
    *   第一个文件处理完结束之后，第二个文件在进来，在按照
    *   第一个文件处理的方式，进行相同的步骤，一个文件，就是一个切片，一个切片就是
    *   一个MapTask，一个MapTask去处理这个文件中的数据，另外一个MapTask在去处理另外
    *   一个文件，两个MapTask处理数据的时候，是并行的，最终两个MapTask都需要走一遍
    *   shuffle，把各自的文件交给一个ReduceTask，或者是多个ReduceTask*/
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (fileName.contains("order")){
            String[] split = line.split("\t");
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else {
            String[] split = line.split("\t");
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        context.write(outK,outV);
    }
}
