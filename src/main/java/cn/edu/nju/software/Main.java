package cn.edu.nju.software;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.spark.MongoSpark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
    private static final String URI = "mongodb://192.168.1.102:27017";
    private static final String DATABASE = "test";
    private static final String COLLECTION = "graph";
    private static final String MONGO_URI = URI + "/" + DATABASE + "." + COLLECTION;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: GraphX <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf()
                .setAppName("GraphX")
                .set("spark.mongodb.input.uri", MONGO_URI)
                .set("spark.mongodb.output.uri", MONGO_URI);
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<Tuple2<Long, Long>, Integer> initial = MongoSpark.load(context)
                .mapToPair(document -> new Tuple2<>(new Tuple2<>((long) document.getString("from").charAt(0), (long) document.getString("to").charAt(0)), document.getInteger("count")));
        JavaPairRDD<Tuple2<Long, Long>, Integer> append = context.textFile(args[0])
                .filter(line -> line.length() > 2)
                .mapToPair(line -> new Tuple2<>(new Tuple2<>((long) line.charAt(0), (long) line.charAt(2)), 1));
        JavaRDD<Edge<Integer>> edges = initial
                .union(append)
                .reduceByKey((i1, i2) -> i1 + i2)
                .map(tuple -> new Edge<>(tuple._1._1(), tuple._1._2(), tuple._2));
        Graph<Long, Integer> graph = Graph.fromEdges(edges.rdd(),
                0L,
                MEMORY_ONLY(),
                MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Long.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        JavaRDD<Document> documents = graph.edges()
                .toJavaRDD()
                .map(edge -> {
                    String from = String.valueOf((char) edge.srcId());
                    String to = String.valueOf((char) edge.dstId());
                    return Document.parse(String.format("{\"_id\": \"%s\", \"from\": \"%s\", \"to\": \"%s\", \"count\": %d}",
                                    from+to,
                                    from,
                                    to,
                                    edge.attr));
                });
        logger.info(documents.count());
        dropCollection(URI, DATABASE, COLLECTION);
        MongoSpark.save(documents);

        context.stop();
    }

    private static void dropCollection(final String uri, final String database, final String collection) {
        new MongoClient(new MongoClientURI(uri)).getDatabase(database).getCollection(collection).drop();
    }
}
