package stu.liuhl.mr.outputformatentend;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, Text> {
    private FSDataOutputStream keyOutput;
    private FSDataOutputStream otherOutput;
    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            keyOutput = fs.create(new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/extend/key.log"));
            otherOutput = fs.create(new Path("/Users/sparkle6979l/Documents/Hadoop_Study/testdir/extend/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, Text val) throws IOException, InterruptedException {
        String mes = text.toString();
        if (mes.contains("key")){
            keyOutput.writeBytes(val+"\n");
        }else{
            otherOutput.writeBytes(val+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(keyOutput);
        IOUtils.closeStream(otherOutput);
    }
}
