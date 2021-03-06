package hadoop.mr.multipleinputs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * this show how to  use MultipleInputs and Join two file by the same key
 * <p>
 * usage:
 * <p>
 * input1 news data:
 * 111#come on
 * 333#go on
 * 222#keep on
 * input2 news comments data:
 * 111#aaa#how
 * 111#bbb#what
 * 222#ccc#why
 * output data:
 * part-m-000000.txt
 * 111#come on#aaa#how#bbb#what
 * 222#keep on#ccc#why
 * 333#go on
 */
public class MultipleInputsJoinMain {
    //args:hadoop-mr\src\main\java\hadoop\mr\multipleinputs\input1\news hadoop-mr\src\main\java\hadoop\mr\multipleinputs\input2\comments output
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MultipleInputsJoinMain <input one path> <input two path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(MultipleInputsJoinMain.class);
        job.setJobName("MultipleInputsJoinMain");

        if (FileSystem.get(job.getConfiguration()).exists(new Path(args[2]))) {
            FileSystem.get(job.getConfiguration()).delete(new Path(args[2]), true);
        }

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NewsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentsMapper.class);

        job.setReducerClass(NewsCommentsReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
