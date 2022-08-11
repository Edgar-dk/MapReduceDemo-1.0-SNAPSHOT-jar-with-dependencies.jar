package com.sias.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-10 20:43
 * @faction:
 */

public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
