import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class TriangleCounting {
    public static void main(String[] args) {
        // Initialize Spark
        String inputFile = args[0];
        SparkConf conf = new SparkConf().setAppName("Triangle Counting");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Read graph
        JavaRDD<String> lines = jsc.textFile(inputFile);

        // Parse graph
        JavaPairRDD<Integer, Data> network = lines.mapToPair(line -> {
            // Find vertex
            String[] firstPair = line.split("=");
            Integer vertex = Integer.parseInt(firstPair[0]);
            String content = firstPair[1];

            // Find neighbors
            List<Integer> neighbors = new ArrayList<>();
            String[] pairs = content.split(";");
            for (String pair : pairs) {
                String[] secondPair = pair.split(",");
                Integer neighbor = Integer.parseInt(secondPair[0]);
                neighbors.add(neighbor);
            }

            // Initialize previous and previous previous vertexes
            List<Integer> previous = new ArrayList<>();
            List<Integer> previousPrevious = new ArrayList<>();

            return new Tuple2<>(vertex, new Data(neighbors, previous, previousPrevious));
        });

        // Execute triangle counting
        Integer result = network
                // First iteration
                // Record current vertex as previous
                // Then step to neighbors
                .flatMapToPair(pair -> {
                    // Map current vertex
                    List<Tuple2<Integer, Data>> results = new ArrayList<>();
                    results.add(pair);

                    // Map neighbors of current vertex
                    // When neighbor has lower rank
                    for (Integer neighbor : pair._2.neighbors) {
                        if (neighbor < pair._1) {
                            results.add(new Tuple2<Integer, Data>(
                                    neighbor,
                                    new Data(new ArrayList<>(), new ArrayList<>(Arrays.asList(pair._1)),
                                            new ArrayList<>())));
                        }
                    }

                    return results.iterator();
                })
                // First iteration
                // Union previous vertexes
                .reduceByKey((data1, data2) -> {
                    // Find neighbors
                    List<Integer> neighbors = data1.neighbors.size() == 0 ? data2.neighbors : data1.neighbors;

                    // Union previous vertexes
                    List<Integer> union = new ArrayList<>(data1.previous);
                    union.addAll(data2.previous);

                    return new Data(neighbors, union, new ArrayList<>());
                })
                // Second iteration
                // Record previous vertex as previous previous
                // Then step to neighbors
                .flatMapToPair(pair -> {
                    // Map current vertex
                    List<Tuple2<Integer, Data>> results = new ArrayList<>();
                    results.add(pair);

                    // Map neighbors of current vertex
                    // When neighbor has lower rank
                    for (Integer neighbor : pair._2.neighbors) {
                        if (neighbor < pair._1) {
                            results.add(new Tuple2<Integer, Data>(
                                    neighbor,
                                    new Data(new ArrayList<>(), new ArrayList<>(), pair._2.previous)));
                        }
                    }

                    return results.iterator();
                })
                // Second iteration
                // Union previous previous vertexes
                .reduceByKey((data1, data2) -> {
                    // Find neighbors
                    List<Integer> neighbors = data1.neighbors.size() == 0 ? data2.neighbors : data1.neighbors;

                    // Find previous
                    List<Integer> previous = data1.previous.size() == 0 ? data2.previous : data1.previous;

                    // Union previous previous vertexes
                    List<Integer> union = new ArrayList<>(data1.previousPrevious);
                    union.addAll(data2.previousPrevious);

                    return new Data(neighbors, previous, union);
                })
                // Count triangles
                .map(pair -> {
                    // Count neighbors of current vertex
                    // When neighbor is previous previous vertex
                    Integer count = 0;
                    for (Integer prevPrevNeighbor : pair._2.previousPrevious) {
                        if (pair._2.neighbors.contains(prevPrevNeighbor)) {
                            count += 1;
                        }
                    }
                    return count;
                })
                .reduce(Integer::sum);

        // Print results
        System.out.printf("Number of Triangles = %d\n", result);

        jsc.stop();
    }
}
