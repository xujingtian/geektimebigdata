package com.xujingtian;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileFlowMapper extends Mapper<LongWritable, Text, Text, MobileFlowBean> {

    // 不能在map方法中new对象，map方法执行频率高，内存消耗大。这也就是需要在bean对象中要有一个空构造方法的原因
    MobileFlowBean bean = new MobileFlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1.获取一行数据（）
        String line = value.toString();

        // 2.截取字段（1、2可归结为一大步：对输入数据的处理）
        String[] fields = line.split("\t");

        // 3.封装bean对象，获取电话号码（第二大步：具体的业务逻辑）
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        // 在map方法中new对象是不好的，因为在输入数据时，每读一行数据，会执行以下map方法，这一造成内存消耗很大
        // FlowBean bean = new FlowBean(upFlow,downFlow);
        bean.set(upFlow, downFlow);
        k.set(phoneNum);

        // 4.写出去（第三大步：将数据输出出去，key和value分别是什么，规定清楚）
        // context.write(new Text(phoneNum), bean);
        context.write(k, bean);
    }
}
