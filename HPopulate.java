import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.StringUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class HPopulate {

    public HPopulate() throws ParseException {}

    /**
     * Enum used to help indexing the csv columns
     */
    public enum FlightField {
        YEAR(0),
        QUARTER(1),
        MONTH(2),
        DAY_OF_MONTH(3),
        DAY_OF_WEEK(4),
        FLIGHT_DATE(5),
        UNIQUE_CARRIER(6),
        AIRLINE_ID(7),
        CARRIER(8),
        TAIL_NUM(9),
        FLIGHT_NUM(10),
        ORIGIN(11),
        ORIGIN_CITY_NAME(12),
        ORIGIN_STATE(13),
        ORIGIN_STATE_FIPS(14),
        ORIGIN_STATE_NAME(15),
        ORIGIN_WAC(16),
        DEST(17),
        DEST_CITY_NAME(18),
        DEST_STATE(19),
        DEST_STATE_FIPS(20),
        DEST_STATE_NAME(21),
        DEST_WAC(22),
        CRS_DEP_TIME(23),
        DEP_TIME(24),
        DEP_DELAY(25),
        DEP_DELAY_MINUTES(26),
        DEP_DEL15(27),
        DEPARTURE_DELAY_GROUPS(28),
        DEP_TIME_BLK(29),
        TAXI_OUT(30),
        WHEELS_OFF(31),
        WHEELS_ON(32),
        TAXI_IN(33),
        CRS_ARR_TIME(34),
        ARR_TIME(35),
        ARR_DELAY(36),
        ARR_DELAY_MINUTES(37),
        ARR_DEL15(38),
        ARRIVAL_DELAY_GROUPS(39),
        ARR_TIME_BLK(40),
        CANCELLED(41),
        CANCELLATION_CODE(42),
        DIVERTED(43),
        CRS_ELAPSED_TIME(44),
        ACTUAL_ELAPSED_TIME(45),
        AIR_TIME(46),
        FLIGHTS(47),
        DISTANCE(48),
        DISTANCE_GROUP(49),
        CARRIER_DELAY(50),
        WEATHER_DELAY(51),
        NAS_DELAY(52),
        SECURITY_DELAY(53),
        LATE_AIRCRAFT_DELAY(54);

        private final int index;

        private FlightField(int index) {
            this.index = index;
        }

        public int getIndex() {
            return this.index;
        }
    }

    public static List<String> splitCSV(String str) {
        List<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int quoteCount = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '"') {
                quoteCount++;
            } else if (c == ',' && quoteCount % 2 == 0) {
                result.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        result.add(sb.toString());
        return result;
    }

    /**
     * Customized writable class used to pass data to reducer and write records
     * hbase table there based on the writable data.
     */
    public static class FlightInfoWritable implements Writable {
        Text airline;
        IntWritable year;
        IntWritable month;
        IntWritable isCancelledOrDiverted;
        DoubleWritable arriveDelayMinutes;

        public FlightInfoWritable() {
            this.airline = new Text();
            this.year = new IntWritable();
            this.month = new IntWritable();
            this.isCancelledOrDiverted = new IntWritable();
            this.arriveDelayMinutes = new DoubleWritable();
        }

        public void set(
                String airline, int year, int month,
                int isCancelledOrDiverted, double arriveDelayMinutes) {
            this.airline = new Text(airline);
            this.year = new IntWritable(year);
            this.month = new IntWritable(month);
            this.isCancelledOrDiverted =
                    new IntWritable(isCancelledOrDiverted);
            this.arriveDelayMinutes = new DoubleWritable(arriveDelayMinutes);
        }

        public void write(DataOutput out) throws IOException {
            this.airline.write(out);
            this.year.write(out);
            this.month.write(out);
            this.isCancelledOrDiverted.write(out);
            arriveDelayMinutes.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            airline.readFields(in);
            year.readFields(in);
            month.readFields(in);
            isCancelledOrDiverted.readFields(in);
            arriveDelayMinutes.readFields(in);
        }

        public String toString() {
            return airline.toString() + "-" + year.toString() + "-" +
                    month.toString() + "-" + isCancelledOrDiverted.toString() +
                    "-" + arriveDelayMinutes.toString();
        }
    }

    public static class FlightDelayTimeMapper
            extends Mapper<LongWritable, Text, Text, FlightInfoWritable> {

        private Text emitKey = new Text();
        private FlightInfoWritable emitValue = new FlightInfoWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * Emit key:   airline-year-month, e.g. AA-2008-01
         * Emit value: FlightInfoWritable. including airline id, year, month, isCancelledOrDiverted, arriveDelay.
         */
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            for (String line : lines) {
                try {
                    List<String> fields = splitCSV(line);
                    String airline =
                            fields.get(FlightField.UNIQUE_CARRIER.getIndex());
                    int year =
                            Integer.parseInt(fields.get(FlightField.YEAR.getIndex()));
                    int month =
                            Integer.parseInt(fields.get(FlightField.MONTH.getIndex()));
                    int isCancelledOrDiverted =
                            (Math.abs(Double.parseDouble(fields.get(FlightField.CANCELLED.getIndex())) - 1.0) < 1e-9 ||
                                    Math.abs(Double.parseDouble(fields.get(FlightField.DIVERTED.getIndex())) - 1.0) < 1e-9) ? 1 : 0;
                    double arrDelayMinutes = 0.0;
                    if (!StringUtil.isEmpty(fields.get(FlightField.ARR_DELAY_MINUTES.getIndex()))) {
                        arrDelayMinutes = Double.parseDouble(fields.get(FlightField.ARR_DELAY_MINUTES.getIndex()));
                    }
                    emitKey.set(airline + "-" + year + "-" + (month < 10 ? "0" : "") + month);
                    emitValue.set(airline, year, month, isCancelledOrDiverted, arrDelayMinutes);
                    context.write(emitKey, emitValue);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            System.out.println("Finish map task " + context.getTaskAttemptID());
        }
    }

    public static class FlightDelayTimeReducer
            extends Reducer<Text, FlightInfoWritable, Text,
            Text> {
        Text emitKey = new Text();
        Text emitValue = new Text();

        private Connection connection;
        private Table table;
        private String tableNameStr;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            tableNameStr = context.getConfiguration().get("hbase.table.name");

            Configuration config = HBaseConfiguration.create();
//            config.set("hbase.zookeeper.quorum", "localhost");
            config.set("hbase.zookeeper.quorum", "172.31.8.202");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            TableName tableName = TableName.valueOf(tableNameStr);
            table = connection.getTable(tableName);
        }

        /**
         * Write records in the same month under the same airline into hbase
         */
        public void reduce(Text key, Iterable<FlightInfoWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Put> putList = new ArrayList<>();
            for (FlightInfoWritable val : values) {
                String airline = val.airline.toString();
                int year = val.year.get();
                int month = val.month.get();
                int isCancelledOrDiverted = val.isCancelledOrDiverted.get();
                double arriveDelayMinutes = val.arriveDelayMinutes.get();
                String rowKey =
                        airline + "-" + year + "-" + (month < 10 ? "0" : "") +
                                month + "-" + UUID.randomUUID();
                String columnFamilyStr = Integer.toString(year);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamilyStr), Bytes.toBytes(
                        "airline"), Bytes.toBytes(airline));
                put.addColumn(Bytes.toBytes(columnFamilyStr), Bytes.toBytes(
                        "year"), Bytes.toBytes(year));
                put.addColumn(Bytes.toBytes(columnFamilyStr), Bytes.toBytes(
                        "month"), Bytes.toBytes(month));
                put.addColumn(Bytes.toBytes(columnFamilyStr), Bytes.toBytes(
                        "isCancelledOrDiverted"), Bytes.toBytes(isCancelledOrDiverted));
                put.addColumn(Bytes.toBytes(columnFamilyStr), Bytes.toBytes(
                        "arriveDelayMinutes"), Bytes.toBytes(arriveDelayMinutes));
                putList.add(put);
            }
            System.out.println("Write records of " + key.toString() + " to " +
                    "table " + tableNameStr);
            // Put a batch of records into hbase table to make the performance better
            table.put(putList);
            emitKey.set(key.toString());
            emitValue.set("test");
            context.write(emitKey, emitValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            table.close();
            connection.close();
        }
    }

    /**
     * Customized partitioner to put records with the same airline id to the same partition
     */
    public static class FlightKeyPartitioner extends Partitioner<Text,
            FlightInfoWritable> {

        @Override
        public int getPartition(Text key, FlightInfoWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CSVToHBase <inputPath> <outputPath> <outputTable>");
            System.exit(0);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String tableNameStr = args[2];
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.quorum", "172.31.8.202");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());
        TableName tableName = TableName.valueOf(tableNameStr);

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("Delete table");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        // Create column family list by year, so that we can use this column family
        // to filter records that are not in 2008.
        List<String> columnFamilyList = Arrays.asList(
                "2000","2001","2002","2003","2004",
                "2005","2006","2007","2008","2009");
        for (String family : columnFamilyList) {
            tableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        // Split the HBase table into multiple regions based on the airline id,
        // so that the data under the same airline will be put into the same region,
        // then all these records will be sent to the same map task.
        byte[][] splits = new byte[][]{
                Bytes.toBytes("9E"),
                Bytes.toBytes("AA"),
                Bytes.toBytes("AQ"),
                Bytes.toBytes("AS"),
                Bytes.toBytes("B6"),
                Bytes.toBytes("CO"),
                Bytes.toBytes("DL"),
                Bytes.toBytes("EV"),
                Bytes.toBytes("F9"),
                Bytes.toBytes("FL"),
                Bytes.toBytes("HA"),
                Bytes.toBytes("MQ"),
                Bytes.toBytes("NW"),
                Bytes.toBytes("OH"),
                Bytes.toBytes("OO"),
                Bytes.toBytes("UA"),
                Bytes.toBytes("US"),
                Bytes.toBytes("WN"),
                Bytes.toBytes("XE"),
                Bytes.toBytes("YV")
        };
        System.out.println("Create table");
        admin.createTable(tableDescriptor, splits);
        connection.close();

        Configuration conf1 = new Configuration();
        conf1.set("hbase.table.name", tableNameStr);
        Job job1 = Job.getInstance(conf1, "Flight avg delay");
        job1.setJarByClass(HPopulate.class);

        job1.setMapperClass(FlightDelayTimeMapper.class);
        job1.setReducerClass(FlightDelayTimeReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FlightInfoWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setPartitionerClass(FlightKeyPartitioner.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        System.out.println("Start job");
        boolean success = job1.waitForCompletion(true);
        System.out.println("Finish job, success : " + (success ? "True" : "False"));
    }
}