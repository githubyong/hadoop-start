package hadoop.mr.average;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * this show how to get sum and average
 * 按key(这里的key是小时)汇总的同时求平均值,主要思路是自定义了一个数据类型
 */
public class CountAverageMain {
    //args:hadoop-mr\src\main\java\hadoop\mr\average\input output
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountAverage <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(CountAverageMain.class);
        job.setJobName("CountAverage");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(job.getConfiguration()).delete(outputPath, true);

        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(IntWritable.class);              //注1
        job.setOutputValueClass(CountAverageTuple.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
