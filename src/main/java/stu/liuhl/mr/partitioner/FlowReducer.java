package stu.liuhl.mr.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long totalupFlow = 0;
        long totaldownFlow = 0;

        for (FlowBean value : values) {
            totalupFlow += value.getUpFlow();
            totaldownFlow += value.getDownFlow();
        }
        outK.set(key);
        outV.setUpFlow(totalupFlow);
        outV.setDownFlow(totaldownFlow);
        outV.setSumFlow();

        context.write(outK,outV);
    }
}
