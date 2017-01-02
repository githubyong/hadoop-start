package hadoop.mr.partitioner;

import hadoop.mr.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * 采样器 How to automatically find “good” partitioning function
 * 合理为reduce任务分配partition
 */
public class TotalSortMR {

    @SuppressWarnings("deprecation")
    public static int runTotalSortJob(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path partitionFile = new Path(args[2]);//写partition 文件的地方
        int reduceNumber = Integer.parseInt(args[3]); //reduce的个数


        // RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
        //示例使用了3个reduce,所以这个sampler会产生2个num:num1,num2 0-num1 对应reduce1;num1-num2 对应reduce2;num2- ~对应reduce3
        RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(1, 3000, 3);
        //frea=1/3 意思是每隔3个采样一个
//        InputSampler.IntervalSampler<Text, Text> sampler = new InputSampler.IntervalSampler<Text, Text>(0.333, 10);


        Configuration conf = new Configuration();
        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

        Job job = Job.getInstance(conf);


        job.setJobName("Total-Sort");
        job.setJarByClass(TotalSortMR.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(reduceNumber);


        //
        job.setPartitionerClass(TotalOrderPartitioner.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        //
        InputSampler.writePartitionFile(job, sampler);

        return job.waitForCompletion(true)? 0 : 1;
//        return 0;
    }

    //hadoop-mr\src\main\java\hadoop\mr\partitioner\input output partest/par.file 3
    public static void main(String[] args) throws Exception {
        System.exit(runTotalSortJob(args));
    }
}
