package stu.liuhl.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceMapper extends Mapper<LongWritable, Text,Text,ReduceBean> {
    private String filename;
    private Text outK = new Text();
    private ReduceBean outV = new ReduceBean();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, ReduceBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ReduceBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(filename.contains("order")){
            String[] split = line.split("\t");
            outK.set(split[1]);
            outV.setTid(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }
        else{
            String[] split = line.split("\t");
            outK.set(split[0]);
            outV.setTid("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        context.write(outK,outV);
    }
}
