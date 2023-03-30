import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.StringUtil;

import java.io.File;
import java.io.IOException;

public class HCompute {

    static final String targetYear = "2008";

    /**
     * Because we split the hbase table into multiple regions by airline id in H populate, the records
     * sent to current map task are all under the same flight; also because the built-in sort of hbase
     * table, we don't need to sort the records again, we just need to iterate the records from start
     * to end and put the average delay of each month into a string, then pass the string to reducer.
     * Emit key:   airline id, e.g. AA
     * Emit value: pairs of month and average delay, e.g. (1,10),(2,20),(3,30)
     */
    public static class MyMapper extends TableMapper<Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private int counter;
        private String targetAirline;
        private int current_month;
        private StringBuilder outputStr;
        private int flightCounter;
        private double totalArrDelay;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            counter = 0;
            current_month = 0;
            outputStr = new StringBuilder();
            flightCounter = 0;
            totalArrDelay = 0.0;
            targetAirline = "";
        }

        /**
         * Each map function call will only deal with one record, you can treat this as
         * an iterator of the records in HBase.
         */
        public void map(ImmutableBytesWritable row, Result value, Context context) {
            counter += 1;
            String airline = Bytes.toString(value.getValue(Bytes.toBytes(
                    targetYear), Bytes.toBytes("airline")));
            if (!airline.equalsIgnoreCase(targetAirline)) {
                targetAirline = airline;
            }
            int month = Bytes.toInt(value.getValue(Bytes.toBytes(
                    targetYear), Bytes.toBytes("month")));
            int isCancelledOrDiverted =
                    Bytes.toInt(value.getValue(Bytes.toBytes(
                            targetYear), Bytes.toBytes("isCancelledOrDiverted")));
            double arriveDelayMinutes =
                    Bytes.toDouble(value.getValue(Bytes.toBytes(
                            targetYear), Bytes.toBytes("arriveDelayMinutes")));

            if (isCancelledOrDiverted == 0 && !StringUtil.isEmpty(airline)) {
                if (month != current_month) {
                    if (current_month > 0) {
                        int avgDelay =
                                (int) Math.ceil(totalArrDelay / flightCounter);
                        outputStr.append("(").append(current_month).append(",").append(avgDelay).append("),");
                        flightCounter = 0;
                        totalArrDelay = 0.0;
                    }
                    current_month = month;
                }
                flightCounter += 1;
                totalArrDelay += arriveDelayMinutes;
            }
        }

        /**
         * Sent the average delay string to reducer at the end of current map task
         */
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            if (!StringUtil.isEmpty(targetAirline)) {
                int avgDelay =
                        (int) Math.ceil(totalArrDelay / flightCounter);
                outputStr.append("(").append(current_month).append(",").append(avgDelay).append(")");
                outputKey.set(targetAirline);
                outputValue.set(outputStr.toString());
                System.out.println("Finish map task for region " + targetAirline + ", airline count " + counter);
                context.write(outputKey, outputValue);
            }
        }
    }

    /**
     * The only thing reducer does is to write the key value pair received from mapper
     * into output file.
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        String tableNameStr = args[0];
        String outputPath = args[1];
        System.out.println(tableNameStr + "-" + outputPath);

        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, tableNameStr);
//        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.quorum", "172.31.8.202");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        config.addResource(new File(hbaseSite).toURI().toURL());
        Job job = Job.getInstance(config, "HBase Table MapReduce");
        job.setJarByClass(HCompute.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(targetYear));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TableInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        TableMapReduceUtil.initTableMapperJob(
                tableNameStr,        // HBase table name
                scan,                // Scan
                MyMapper.class,      // Mapper
                Text.class,          // Mapper out key
                Text.class,          // Mapper out value
                job);                // Job

        System.out.println("execute job");
        job.waitForCompletion(true);
        System.out.println("finished job");
    }
}