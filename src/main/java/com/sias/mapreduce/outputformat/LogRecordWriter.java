package com.sias.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-11 9:06
 * @faction:
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream siasOut;
    private  FSDataOutputStream otherOut;

    /*1.利用构造器去创建两条流*/
    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {

            /*01.获取文件系统，
            *    并且创建两个输出流，到不同的文件系统*/
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            siasOut = fs.create(new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\outformat\\sias.txt"));
            otherOut = fs.create(new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\outformat\\other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        if (log.contains("atguigu")){
            siasOut.writeBytes(log);
        }else {
            otherOut.writeBytes(log);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(siasOut);
        IOUtils.closeStream(otherOut);
    }
}
