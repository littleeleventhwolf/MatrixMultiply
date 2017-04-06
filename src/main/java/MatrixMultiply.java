import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 17-4-6.
 */
public class MatrixMultiply {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        String filename = null;
        Integer Arow, Bcolumn;

        /**
         * Map阶段全局常量的初始化
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            // 这里获取三个常量：输入文件名、A矩阵行数、B矩阵列数
            filename = ((FileSplit) inputSplit).getPath().getName();
            Arow = context.getConfiguration().getInt("row", 0);
            Bcolumn = context.getConfiguration().getInt("column", 0);
        }

        /**
         * Map阶段的处理逻辑
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\u0009");
            int num = data.length;
            // 定义C矩阵的标签
            StringBuffer keyTag = new StringBuffer();
            keyTag.append("C_");
            // 定义C矩阵的元素值
            StringBuffer matrixValue = new StringBuffer();
            if(num > 0) {
                // 获取矩阵输入行序号
                String k = data[0];
                for(int i = 1; i < num; i++) {
                    matrixValue.append(data[i]);
                    matrixValue.append(":");
                }
                matrixValue.deleteCharAt(matrixValue.length() - 1);
                if(filename.equals("matrixA")) {
                    keyTag.append(k).append("_");
                    for(int ii = 1; ii <= Bcolumn; ii++) {
                        context.write(new Text(keyTag.toString() + ii),
                                new Text(matrixValue.toString()));
                    }
                } else {
                    for(int jj = 1; jj <= Arow; jj++) {
                        context.write(new Text(keyTag.toString() + jj + "_" + k),
                                new Text(matrixValue.toString()));
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        /**
         * reduce阶段的矩阵相乘
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            StringBuffer MulMatrix = new StringBuffer();
            for(Text val : values) {
                MulMatrix.append(val.toString());
                MulMatrix.append("=");
            }
            String[] data = MulMatrix.toString().split("=");
            String[] aMatrix = data[0].split(":");
            String[] bMatrix = data[1].split(":");
            for(int i = 0; i < aMatrix.length; i++) {
                sum += Double.parseDouble(aMatrix[i]) * Double.parseDouble(bMatrix[i]);
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    /**
     * 驱动阶段的处理
     * @param input
     * @param output
     * @param Arow
     * @param Bcolumn
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static Boolean run(String input, String output, Integer Arow, Integer Bcolumn)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        // 初始化全局常量
        conf.setInt("row", Arow);
        conf.setInt("column", Bcolumn);
        Job job = Job.getInstance(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        Boolean result = job.waitForCompletion(true);
        return result;
    }

    /**
     * 主执行模块
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        run("input/matrixA,input/matrixB", "file:///tmp/result", 4, 5);
    }

}
