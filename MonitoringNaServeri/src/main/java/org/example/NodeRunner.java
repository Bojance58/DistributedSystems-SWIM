package org.example;

import gossip.GossipManager;
import gossip.NodeInfo;
import gossip.NodeState;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class NodeRunner {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("Usage: java -jar app.jar <port> [seedHost1:seedPort1] ...");
            System.exit(1);
        }

        String host = "127.0.0.1";
        int port = Integer.parseInt(args[0]);

        List<String> seedNodes = Arrays.stream(args)
                .skip(1)
                .collect(Collectors.toList());

        // startuvanje na GossipManager
        GossipManager manager = new GossipManager(host, port, seedNodes.toArray(new String[0]));

        System.out.println("Node " + manager.getLocalNodeId() + " is started and active.");
        System.out.println("Seed nodes: " + seedNodes);
        System.out.println("-----------------------------------------------------");
        System.out.println("Commands:");
        System.out.println("  status      - прикажи ја membership листата");
        System.out.println("  ring        - ребилдај hash ring (само ALIVE nodes)");
        System.out.println("  keys        - прикажи кој node е одговорен за неколку пример клучеви");
        System.out.println("  exit        - гаси го node-от");
        System.out.println("-----------------------------------------------------");

        Scanner scanner = new Scanner(System.in);

        try {
            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine().trim().toLowerCase();

                if (line.equals("exit")) {
                    break;
                } else if (line.equals("status")) {
                    printClusterStatus(manager);
                } else if (line.equals("ring")) {
                    manager.rebuildHashRing();
                } else if (line.equals("keys")) {
                    printKeyAssignments(manager);
                } else {
                    System.out.println("Unknown command. Available: status, ring, keys, exit");
                }
            }
        } finally {
            manager.shutdown();
            System.out.println("Node " + manager.getLocalNodeId() + " shut down.");
        }
    }

    // pecatenje na momentalna membership lista
    private static void printClusterStatus(GossipManager manager) {
        Map<String, NodeInfo> cluster = manager.getClusterStatus();
        System.out.println("Current membership (" + cluster.size() + " nodes):");

        for (NodeInfo info : cluster.values()) {
            String mark;
            if (info.getState() == NodeState.ALIVE) {
                mark = "[ALIVE] ";
            } else if (info.getState() == NodeState.SUSPECT) {
                mark = "[SUSPECT] ";
            } else {
                mark = "[DEAD] ";
            }
            System.out.println("  " + mark + info.getId()
                    + " | hb=" + info.getHeartbeat()
                    + " | ts=" + info.getTimestamp());
        }
    }

    // pokazuva koj node e odgovoren za nekolku primer klucevi preku Hash Ring
    private static void printKeyAssignments(GossipManager manager) {
        String[] sampleKeys = {"cpu:nodeA", "cpu:nodeB", "memory:job1", "request:12345", "request:67890"};

        System.out.println("Key assignments (via Consistent Hash Ring):");
        for (String key : sampleKeys) {
            String nodeId = manager.findResponsibleNode(key);
            System.out.println("  key='" + key + "' -> node=" + nodeId);
        }
    }
}
