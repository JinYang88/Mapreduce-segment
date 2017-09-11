import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.huaban.analysis.jieba.*;
import org.apache.commons.lang3.StringUtils;

public class WordCount {

    private static int TargetFieldNum;

    private static Text join_field(Vector<String> OldVec){
        StringBuilder sb = new StringBuilder();

        for(int j=0;j<OldVec.size()-1;j++){
            sb.append('"');
            sb.append(OldVec.get(j));
            sb.append("\",");
        }
        sb.append('"');
        sb.append(OldVec.get(OldVec.size()-1));
        sb.append('"');

        return new Text(sb.toString());
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, NullWritable, Text> {


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String OldText = value.toString();


            line_parser lp = new line_parser();
            if(lp.readCSVline(OldText)){
                Vector<String> fieldsave = lp.vContent;

                JiebaSegmenter segmenter = new JiebaSegmenter();
                List<SegToken> temp = segmenter.process(fieldsave.get(TargetFieldNum), JiebaSegmenter.SegMode.SEARCH);

                StringBuilder sb = new StringBuilder();
                for(SegToken item : temp){
                    sb.append(item.word);
                    sb.append(" ");
                }
                String res = sb.toString();

                fieldsave.set(TargetFieldNum, res);

                context.write(NullWritable.get(), join_field(fieldsave));
            }
        }
    }

//    public static class IntSumReducer
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
//    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        TargetFieldNum = Integer.parseInt(args[2]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
