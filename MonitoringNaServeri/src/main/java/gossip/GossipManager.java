package gossip;

import hashing.ConsistentHashRing;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class GossipManager {

    private final UDPGossipService gossipService;
    private final ConsistentHashRing<String> hashRing;

    private final ConcurrentMap<String, String> localAssignments = new ConcurrentHashMap<>();

    public GossipManager(String host, int port, String... seedNodes) throws IOException {

        this.gossipService = new UDPGossipService(host, port, java.util.Arrays.asList(seedNodes));

        // Иницијален прстен со сите моментално познати јазли
        java.util.Collection<String> initialNodes = gossipService.getMembershipList().keySet();
        this.hashRing = new ConsistentHashRing<>(100, initialNodes);

        this.gossipService.start();
    }

    /**
     * Го реконструира Consistent Hash Ring-от користејќи ги само јазлите во ALIVE состојба.
     */
    public void rebuildHashRing() {
        // 1. Земи ја моменталната листа на членство
        Map<String, NodeInfo> currentMembers = gossipService.getMembershipList();

        // 2. Филтрирај ги само ALIVE јазлите
        List<String> aliveNodeIds = currentMembers.values().stream()
                .filter(node -> node.getState() == NodeState.ALIVE)
                .map(NodeInfo::getId)
                .collect(Collectors.toList());

        // 3. Реконструирај го Hash Ring-от
        hashRing.rebuild(aliveNodeIds);

        System.out.println("Consistent Hash Ring rebuilt successfully with " + aliveNodeIds.size() + " ALIVE nodes.");
    }

    public java.util.Map<String, NodeInfo> getClusterStatus() {
        return gossipService.getMembershipList();
    }

    public String getLocalNodeId() {
        return gossipService.getSelfId();
    }

    public void shutdown() {
        gossipService.shutdown();
    }

    public String findResponsibleNode(String key) {
        return hashRing.getNode(key);
    }
}