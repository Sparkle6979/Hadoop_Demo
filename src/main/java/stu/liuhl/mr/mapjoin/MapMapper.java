package stu.liuhl.mr.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private HashMap<String,String> rec = new HashMap<>();
    private Text outK = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        FileSystem fs = FileSystem.get(context.getConfiguration());

        URI[] cacheFiles = context.getCacheFiles();
        FSDataInputStream file = fs.open(new Path(cacheFiles[0]));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));

        String line;
        while(StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] split = line.split("\t");
            rec.put(split[0],split[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        outK.set(split[0] + "\t" + rec.get(split[1]) + "\t" + split[2]);

        context.write(outK,NullWritable.get());
    }
}
