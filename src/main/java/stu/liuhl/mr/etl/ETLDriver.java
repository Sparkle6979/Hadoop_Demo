package stu.liuhl.mr.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);

        // 取消reduce阶段记得Task设为0
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/data/web.log"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/webres"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
