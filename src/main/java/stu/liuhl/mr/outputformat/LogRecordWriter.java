package stu.liuhl.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream keyOutput;
    private FSDataOutputStream otherOutput;
    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            keyOutput = fs.create(new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/key.log"));
            otherOutput = fs.create(new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String mes = text.toString();
        if (mes.contains("atguigu")){
            keyOutput.writeBytes(mes+"\n");
        }else{
            otherOutput.writeBytes(mes+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(keyOutput);
        IOUtils.closeStream(otherOutput);
    }
}
