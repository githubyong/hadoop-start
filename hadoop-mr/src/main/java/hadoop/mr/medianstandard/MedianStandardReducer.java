package hadoop.mr.medianstandard;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by baoyu on 16/10/22.
 */
public class MedianStandardReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStandardTuple> {

    private MedianStandardTuple result = new MedianStandardTuple();

    private ArrayList<Float> commentLengths = new ArrayList();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;
        commentLengths.clear();
        result.setStandard(0);
        for (IntWritable val : values) {
            commentLengths.add((float) val.get());
            sum += val.get();
            ++count;
        }

        Collections.sort(commentLengths);
        //average : even value,average midlle two elements
        //假如有n个数据，当n为偶数时，中位数为第n/2位数和第(n+2)/2位数的平均数；如果n为奇数，那么中位数为第（n+1）/2位数的值
        if (count % 2 == 0) {
            result.setMedian((commentLengths.get((int) count / 2 - 1) + commentLengths.get((int) count / 2) / 2.0f));
        } else {
            result.setMedian(commentLengths.get((int) count / 2));
        }

        //standard deviation
        float mean = sum / count;
        float sumOfSquares = 0.0f;
        for (Float f : commentLengths) {
            sumOfSquares += (f - mean) * (f - mean);
        }
        result.setStandard((float) Math.sqrt(sumOfSquares / (count - 1)));

        context.write(key, result);
    }
}
