import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.StringUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Secondary {

    public Secondary() throws ParseException {}

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
     * Key used to represent each record, it is writable and comparable.
     * Members: airline id, month
     */
    public static class FlightKey implements WritableComparable<FlightKey> {

        private String airline;
        private int month;

        public FlightKey() {}

        public FlightKey(String airline, int month) {
            this.airline = airline;
            this.month = month;
        }

        public void set(String airline, int month) {
            this.airline = airline;
            this.month = month;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(airline);
            dataOutput.writeInt(month);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            airline = dataInput.readUTF();
            month = dataInput.readInt();
        }

        @Override
        public int compareTo(FlightKey o) {
            int result = airline.compareTo(o.airline);
            if (result != 0) {
                return result;
            }
            return Integer.compare(month, o.month);
        }

        @Override
        public String toString() {
            return airline + "-" + month;
        }
    }

    public static class FlightDataWritable implements Writable {

        private IntWritable month;
        private DoubleWritable arrDelayMinute;

        public FlightDataWritable() {
            this.month = new IntWritable();
            this.arrDelayMinute = new DoubleWritable();
        }

        public void set(int month, double arrDelayMinute) {
            this.month.set(month);
            this.arrDelayMinute.set(arrDelayMinute);
        }

        public void write(DataOutput out) throws IOException {
            month.write(out);
            arrDelayMinute.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            month.readFields(in);
            arrDelayMinute.readFields(in);
        }

        public String toString() {
            return month + "-" + arrDelayMinute;
        }
    }


    public static class FlightDelayTimeMapper
            extends Mapper<LongWritable, Text, FlightKey, FlightDataWritable> {
        private FlightKey flightKey = new FlightKey();
        private FlightDataWritable emitValue = new FlightDataWritable();

        private boolean isValidFlight(String line) {
            List<String> fields = splitCSV(line);
            List<Integer> targetIndexes = Arrays.asList(
                    FlightField.AIRLINE_ID.getIndex(),
                    FlightField.YEAR.getIndex(),
                    FlightField.MONTH.getIndex(),
                    FlightField.DEP_TIME.getIndex(),
                    FlightField.ARR_DELAY_MINUTES.getIndex()
            );
            for (int index : targetIndexes) {
                if (StringUtil.isEmpty(fields.get(index))) {
                    return false;
                }
            }
            if (Integer.parseInt(fields.get(FlightField.YEAR.getIndex())) != 2008) {
                return false;
            }
            return true;
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            for (String line : lines) {
                if (isValidFlight(line)) {
                    try {
                        List<String> fields = splitCSV(line);
                        String airline =
                                fields.get(FlightField.UNIQUE_CARRIER.getIndex());
                        int month =
                                Integer.parseInt(fields.get(FlightField.MONTH.getIndex()));
                        double arrDelayMinutes =
                                Double.parseDouble(fields.get(FlightField.ARR_DELAY_MINUTES.getIndex()));
                        flightKey.set(airline, month);
                        emitValue.set(month, arrDelayMinutes);
                        context.write(flightKey, emitValue);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
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
            extends Reducer<FlightKey, FlightDataWritable, Text, Text> {
        Text emitKey = new Text();
        Text emitValue = new Text();

        public void reduce(FlightKey key, Iterable<FlightDataWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Compute the average delay in current month for current airline,
            // then store it in the global hashmap in current reducer.
            int count = 0;
            double totalDelay = 0.0;
            int current_month = 0;
            StringBuilder sb = new StringBuilder();
            for (FlightDataWritable val : values) {
                // Iterate the value list, write average delay and corresponding month into output
                // string when we finish the current month.
                // Since the list has been sorted in SortComparator, we don't need to worry about
                // the order of month.
                if (val.month.get() != current_month) {
                    // Make sure we are writing valid record into output, if current month <= 0, it means
                    // the beginning of for loop so no need to write anything.
                    if (current_month > 0) {
                        int avgDelay = (int) Math.ceil(totalDelay / count);
                        sb.append("(").append(current_month).append(",").append(avgDelay).append("),");
                    }
                    current_month = val.month.get();
                    count = 0;
                    totalDelay = 0.0;
                }
                count += 1;
                totalDelay += val.arrDelayMinute.get();
            }
            // Write the data of last month in list into output
            int avgDelay = (int) Math.ceil(totalDelay / count);
            sb.append("(").append(current_month).append(",").append(avgDelay).append(")");

            emitKey.set(key.airline);
            emitValue.set(sb.toString());
            System.out.println("Finish reduce function call " + emitKey + " :" +
                    " " + emitValue);
            context.write(emitKey, emitValue);
        }
    }

    /**
     * Group comparator used to send records of the same airline id to the same reducer
     */
    public static class FlightGroupingComparator extends WritableComparator {

        public FlightGroupingComparator() {
            super(FlightKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            FlightKey fk1 = (FlightKey) a;
            FlightKey fk2 = (FlightKey) b;
            return fk1.airline.compareTo(fk2.airline);
        }
    }

    /**
     * Sort comparator used to sort the records by month
     * Because we have already filtered records whose year != 2008,
     * we don't need to care about the year
     */
    public static class FlightSortComparator extends WritableComparator {

        public FlightSortComparator() {
            super(FlightKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            FlightKey fk1 = (FlightKey) a;
            FlightKey fk2 = (FlightKey) b;
            if (fk1.airline.compareTo(fk2.airline) != 0) {
                return fk1.airline.compareTo(fk2.airline);
            }
            return Integer.compare(fk1.month, fk2.month);
        }
    }

    /**
     * Customized partitioner to put records with the same airline id to the same partition
     */
    public static class FlightKeyPartitioner extends Partitioner<FlightKey,
            FlightDataWritable> {

        @Override
        public int getPartition(FlightKey key, FlightDataWritable value, int numPartitions) {
            return (key.airline.hashCode()) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Flight avg delay");
        job1.setJarByClass(Secondary.class);

        job1.setMapperClass(FlightDelayTimeMapper.class);
        job1.setReducerClass(FlightDelayTimeReducer.class);

        job1.setMapOutputKeyClass(FlightKey.class);
        job1.setMapOutputValueClass(FlightDataWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setPartitionerClass(FlightKeyPartitioner.class);
        job1.setSortComparatorClass(FlightSortComparator.class);
        job1.setGroupingComparatorClass(FlightGroupingComparator.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    }
}