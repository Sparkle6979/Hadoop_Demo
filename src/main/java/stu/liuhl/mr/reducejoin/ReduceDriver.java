package stu.liuhl.mr.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 获取jar包路径
        job.setJarByClass(ReduceDriver.class);

        // 3. 关联mapper和reducer
        job.setMapperClass(ReduceMapper.class);
        job.setReducerClass(ReduceReducer.class);

        // 4. 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReduceBean.class);

        // 5. 设置最终输出的kv类型
        job.setOutputKeyClass(ReduceBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6. 设置输入与输出路径
        FileInputFormat.setInputPaths(job,new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/data/"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/tableres4"));

        // 7. 提交job
        boolean result = job.waitForCompletion(true);
        System.exit((result?0:1));

    }
}
