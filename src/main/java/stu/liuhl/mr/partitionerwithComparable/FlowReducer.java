package stu.liuhl.mr.partitionerwithComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean,Text,Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            outK.set(value);
            outV.setUpFlow(key.getUpFlow());
            outV.setDownFlow(key.getDownFlow());
            outV.setSumFlow();
            context.write(outK,outV);
        }
    }
}
