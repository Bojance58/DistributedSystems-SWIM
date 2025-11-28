
package org.example;

import gossip.UDPGossipService;
import java.util.Arrays;
import java.util.List;

import java.util.stream.Collectors;

public class NodeRunner {
    public static void main(String[] args) throws Exception {

        // Проверка дали е даден барем еден аргумент (Порт)
        if (args.length < 1) {

            System.exit(1);
        }

        String host = "127.0.0.1";
        // Првиот аргумент е портот
        int port = Integer.parseInt(args[0]);

        // Ги зема сите аргументи по првиот како Seed Nodes
        List<String> seedNodes = Arrays.stream(args)
                .skip(1)
                .collect(Collectors.toList());

        // Иницијализација и стартување на Gossip Service
        UDPGossipService node = new UDPGossipService(host, port, seedNodes);
        node.start();

        System.out.println("Node " + node.getSelfId() + " is started and active.");
        System.out.println("-----------------------------------------------------");
        System.out.println("Press Ctrl+C to shut down this node.");

        // Го држиме главниот процес активен за да не се исклучи јазолот
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            // Кога ќе се притисне Ctrl+C, ја исклучуваме Gossip услугата
            Thread.currentThread().interrupt();
        } finally {
            node.shutdown();
        }
    }
}