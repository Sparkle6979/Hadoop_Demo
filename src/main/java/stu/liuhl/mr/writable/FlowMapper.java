package stu.liuhl.mr.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String phone = words[1];

        outK.set(phone);
        outV.setDownFlow(Long.parseLong(words[words.length-2]));
        outV.setUpFlow(Long.parseLong(words[words.length-3]));
        outV.setSumFlow();

        context.write(outK,outV);
    }
}
