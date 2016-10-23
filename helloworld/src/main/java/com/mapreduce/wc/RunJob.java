package com.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Mark Tao on 2016/10/22 15:00.
 */


public class RunJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(RunJob.class);
        job.setJobName("wc");

        job.setMapperClass(WordCounteMapper.class);
        job.setReducerClass(WorldCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/root/first/"));

        //输出路径一定是要不存在的，要是存在就有问题了
        Path outpath = new Path("/root/output/wc");

        FileSystem fs = FileSystem.get(config);

        //这里手工判断一次输出路径是否存在，存在就删除掉
        if (fs.exists(outpath)) {
            fs.delete(outpath,true);
        }

        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        if (result) {
            System.out.println("job was done!");
        }
    }
}
