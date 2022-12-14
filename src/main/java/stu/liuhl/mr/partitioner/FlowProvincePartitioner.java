package stu.liuhl.mr.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class FlowProvincePartitioner extends Partitioner<Text,FlowBean> {


    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
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
