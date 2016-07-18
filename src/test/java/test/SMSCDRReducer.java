package test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by root on 2016/7/14.
 */
public class SMSCDRReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }

}

