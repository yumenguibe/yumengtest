/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.javamavenproject;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 *
 * @author mengyu
 */
public class classDemo {
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

  //  private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");

     //skip the first line and get the 3rd and 6rd coloumn data in each line
        String a = itr.nextToken();
        String b = itr.nextToken();
        String c = itr.nextToken();
        String d = itr.nextToken();
        String e = itr.nextToken();
        // make sure the 6rd column is not null
        if (itr.hasMoreTokens()) {
            String s = itr.nextToken();
            //skip the first line
            if (!s.contains("Daily")) {
                double f = Double.valueOf(s);
                word.set(c);
                //get the year and number
                context.write(word, new DoubleWritable(f));
            }
        }
    }
  }

  public static class IntSumReducer
     extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private DoubleWritable result2 = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context1
                       ) throws IOException, InterruptedException {
        double min = 99999.99;
        double max = 0;
        for (DoubleWritable val : values) {
            // get max
            if (max < val.get()) {
                max = val.get();
            }
            // get min
            if (min > val.get()) {
                min = val.get();
            }
        }
      result.set(max);
      result2.set(min);
      context1.write(key, result);
      context1.write(key, result2);
      //context2.write(key, result2);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "minmax");
    job.setJarByClass(classDemo.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
