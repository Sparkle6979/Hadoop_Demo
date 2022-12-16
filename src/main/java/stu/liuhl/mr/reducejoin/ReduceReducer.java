package stu.liuhl.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceReducer extends Reducer<Text,ReduceBean,ReduceBean, NullWritable> {
    private ReduceBean outK = new ReduceBean();

    @Override
    protected void reduce(Text key, Iterable<ReduceBean> values, Reducer<Text, ReduceBean, ReduceBean, NullWritable>.Context context) throws IOException, InterruptedException {

        ArrayList<ReduceBean> ls = new ArrayList<>();
        ReduceBean tb = new ReduceBean();

        for (ReduceBean value : values) {
            if("order".equals(value.getFlag())) {
                ReduceBean tmp = new ReduceBean();
                try {
                    BeanUtils.copyProperties(tmp, value);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                ls.add(tmp);
            }
            else {
                try {
                    BeanUtils.copyProperties(tb, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (ReduceBean l : ls) {
            l.setPname(tb.getPname());
            context.write(l,NullWritable.get());
        }
    }
}
