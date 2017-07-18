package com.futhead.java.spark;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by futhead on 17-7-15.
 */
public final class LogQuery {

    private static List<String> exampleApacheLogs = Lists.newArrayList(
            "10.10.10.10 futhead \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg " +
                    "HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                    "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                    ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                    "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" " +
                    "62.24.11.25 images.com 1358492167 - Whatup",
            "10.10.10.10 futhead \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg " +
                    "HTTP/1.1\" 304 306 \"http:/referall.com\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; " +
                    "GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR " +
                    "3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR  " +
                    "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" " +
                    "0 73.23.2.15 images.com 1358492557 - Whatup"
    );

    private static final Pattern apacheLogRegex = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");

    public static class Stats implements Serializable {

        private final int count;

        private final int numBytes;

        private Stats(int count, int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }

        private Stats merge(Stats other) {
            return new Stats(count + other.count, numBytes + other.numBytes);
        }

        @Override
        public String toString() {
            return "Stats{" +
                    "count=" + count +
                    ", numBytes=" + numBytes +
                    '}';
        }
    }

    private static Tuple3<String, String, String> extractKey(String line) {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(1);
            String user = m.group(2);
            String query = m.group(5);
            if (!user.equalsIgnoreCase("-")) {
                return new Tuple3<String, String, String>(ip, user, query);
            }
        }
        return new Tuple3<String, String, String>(null, null, null);
    }

    private static Stats extratStatus(String line) {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            int bytes = Integer.parseInt(m.group(7));
            return new Stats(1, bytes);
        } else {
            return new Stats(1, 0);
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("LogQuery").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> dataSet = jsc.parallelize(exampleApacheLogs);

        JavaPairRDD<Tuple3<String, String, String>, Stats> extracted = dataSet.mapToPair(new PairFunction<String, Tuple3<String, String, String>, Stats>() {
            public Tuple2<Tuple3<String, String, String>, Stats> call(String s) throws Exception {
                return new Tuple2<Tuple3<String, String, String>, Stats>(extractKey(s), extratStatus(s));
            }
        });

        JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted.reduceByKey(new Function2<Stats, Stats, Stats>() {
            public Stats call(Stats v1, Stats v2) throws Exception {
                return v1.merge(v2);
            }
        });

        List<Tuple2<Tuple3<String, String, String>, Stats>> output = counts.collect();
        for (Tuple2<?, ?> t : output) {
            System.out.println(t._1() + "\t" + t._2());
        }

        spark.stop();

    }
}
