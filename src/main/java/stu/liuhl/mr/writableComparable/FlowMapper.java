package stu.liuhl.mr.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean, Text> {
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean,Text>.Context context) throws IOException, InterruptedException {
        String mes = value.toString();
        String[] split = mes.split("\t");

        String phone = split[0];
        String upflow = split[1];
        String downflow = split[2];

        outK.setUpFlow(Long.parseLong(upflow));
        outK.setDownFlow(Long.parseLong(downflow));
        outK.setSumFlow();
        outV.set(phone);

        context.write(outK,outV);
    }
}
