package stu.liuhl.mr.partitionerwithComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class FlowProvincePartitioner extends Partitioner<FlowBean, Text> {


    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String mes = text.toString();
        String subphone = mes.substring(0,3);

        int partitioner;
        // 常量字符串.equals：防止空指针
        if("136".equals(subphone)) {
            partitioner = 0;
        }
        else if("137".equals(subphone)) {
            partitioner = 1;
        }
        else if("138".equals(subphone)) {
            partitioner = 2;
        }
        else if("139".equals(subphone)){
            partitioner = 3;
        }
        else{
            partitioner = 4;
        }
        return partitioner;
    }

}
