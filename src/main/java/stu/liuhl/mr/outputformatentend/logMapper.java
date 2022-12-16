package stu.liuhl.mr.outputformatentend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class logMapper extends Mapper<LongWritable, Text,Text,Text> {
    private Text outK = new Text();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String mes = value.toString();
        if(mes.contains("atguigu")){
            outK.set("key");
        }else{
            outK.set("other");
        }
        outV.set(value);
        context.write(outK,outV);
    }
}
