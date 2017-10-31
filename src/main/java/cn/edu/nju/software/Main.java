package cn.edu.nju.software;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.bson.Document;
import scala.Tuple2;

import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;

/**
 * @author dalec
 */
public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: GraphX <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf()
                .setAppName("GraphX")
                .set("spark.mongodb.input.uri", "mongodb://192.168.1.102:27017/test.graph")
                .set("spark.mongodb.output.uri", "mongodb://192.168.1.102:27017/test.graph");
        WriteConfig writeConfig = WriteConfig.create(conf);
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<Edge<Integer>> edges = context.textFile(args[0])
                .filter(line -> line.length() > 2)
                .mapToPair(line -> new Tuple2<>(new Tuple2<>(line.charAt(0), line.charAt(2)), 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .map(tuple -> new Edge<>(tuple._1._1, tuple._1._2, tuple._2));
        Graph<Long, Integer> graph = Graph.fromEdges(edges.rdd(),
                0L,
                MEMORY_ONLY(),
                MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Long.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        JavaRDD<Document> documents = graph.edges()
                .toJavaRDD()
                .map(edge ->
                        Document.parse(String.format("{\"from\": \"%s\", \"to\": \"%s\", \"count\": %d}",
                                String.valueOf((char) edge.srcId()),
                                String.valueOf((char) edge.dstId()),
                                edge.attr)));
        MongoSpark.save(documents, writeConfig);

        context.stop();
    }
}
