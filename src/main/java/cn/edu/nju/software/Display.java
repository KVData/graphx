package cn.edu.nju.software;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Display {
    private static Graph createGraph(List<String> nodes, List<Tuple3<String, String, String>> edges) {
        Graph graph = new SingleGraph("GraphX");
        for (String node : nodes) {
            graph.addNode(node);
        }
        for (Tuple3<String, String, String> edge : edges) {
            graph.addEdge(edge._1()+edge._2()+edge._3(), edge._1(), edge._2());
        }
        return graph;
    }

    private static void changeUI(Graph graph) {
        for (Node node : graph) {
            node.addAttribute("ui.label", node.getId());
        }
        for (org.graphstream.graph.Edge edge: graph.getEachEdge()) {
            edge.addAttribute("ui.label", edge.getId());
        }
    }

    public static void display(List<String> nodes, List<Tuple3<String, String, String>> edges) {
        Graph graph = createGraph(nodes, edges);
        changeUI(graph);
        graph.display();
    }

    public static void main(String[] args) {
        List<String> vertices = new ArrayList<>();
        vertices.add("速");
        vertices.add("度");
        vertices.add("很");
        vertices.add("快");
        List<Tuple3<String, String, String>> edges = new ArrayList<>();
        edges.add(new Tuple3<>("速", "度", "2"));
        edges.add(new Tuple3<>("度", "很", "1"));
        edges.add(new Tuple3<>("很", "快", "1"));
        display(vertices, edges);
    }
}
