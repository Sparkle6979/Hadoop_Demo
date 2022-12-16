package stu.liuhl.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException, URISyntaxException {

        // 获取Job
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        // 获取jar包路径
        job.setJarByClass(MapDriver.class);
        job.setMapperClass(MapMapper.class);


        // 设置 map kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置 输出 kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/datatmp/pd.txt"));
        job.setNumReduceTasks(0);

        // 设置 输入 路径
        FileInputFormat.setInputPaths(job,new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/datatmp/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/mapjoinres"));

        // 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit((result?0:1));
    }
}
