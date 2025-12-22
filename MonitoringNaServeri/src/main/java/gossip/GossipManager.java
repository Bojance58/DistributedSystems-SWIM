package gossip;

import hashing.ConsistentHashRing;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class GossipManager {

    // servis za gossip i membership (swim logika preko udp)
    private final UDPGossipService gossipService;
    // consistent hash ring za raspredelba na keys po jazli
    private final ConsistentHashRing<String> hashRing;

    // lokalna mapa: primer key -> node, ako sakame da cashirame raspredelbi
    private final ConcurrentMap<String, String> localAssignments = new ConcurrentHashMap<>();
    // scheduler sto periodichno proveruva dali treba da se rebuilda hash ringot
    private final ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();

    // lista od alive jazli sto bila koristena pri posledniot rebuild na ringot
    private volatile List<String> lastKnownAliveNodes;

    // konstruktor: startuva gossip servis i inicijalen hash ring
    public GossipManager(String host, int port, String... seedNodes) throws IOException {

        // kreira lokalен swim node so zadadeni seed nodes
        this.gossipService = new UDPGossipService(host, port, java.util.Arrays.asList(seedNodes));

        // inicijalen ring so site vo momentot poznati jazli od membership listata
        java.util.Collection<String> initialNodes = gossipService.getMembershipList().keySet();
        this.hashRing = new ConsistentHashRing<>(100, initialNodes);

        // startuvanje na gossip thread-ovite
        this.gossipService.start();

        // na sekoe 5 sekundi proveruvame dali se smenil setot na alive jazli i po potreba rebuild
        rebalanceScheduler.scheduleAtFixedRate(this::checkAndRebuildHashRing, 5, 5, TimeUnit.SECONDS);
    }

    // proveruva dali ima promena vo alive jazlite i ako ima go rebuilda hash ringot
    private void checkAndRebuildHashRing() {
        // 1. zemi trenutna lista na alive jazli od gossip servisot
        List<String> currentAliveNodeIds = gossipService.getMembershipList().values().stream()
                .filter(node -> node.getState() == NodeState.ALIVE)
                .map(NodeInfo::getId)
                .collect(Collectors.toList());

        // 2. proveri dali lista-ta se razlikuva od prethodno zapametenite alive jazli
        if (currentAliveNodeIds.size() != lastKnownAliveNodes.size() ||
                !new HashSet<>(currentAliveNodeIds).equals(new HashSet<>(lastKnownAliveNodes))) {

            System.out.println("-----> Cluster change detected! Rebuilding Hash Ring. <-----");

            // 3. ako ima promena, povika rebuild na ringot so novite alive jazli
            hashRing.rebuild(currentAliveNodeIds);

            // 4. zacuvaj ja novata lista kako lastKnownAliveNodes
            lastKnownAliveNodes = currentAliveNodeIds;
        }
    }

    // rachen rebuild na hash ringot, povikan od /cluster/rebalance endpointot
    public void rebuildHashRing() {
        // 1. zemi ja celata membership mapa
        Map<String, NodeInfo> currentMembers = gossipService.getMembershipList();

        // 2. ostavi gi samo jazlite so sostojba ALIVE
        List<String> aliveNodeIds = currentMembers.values().stream()
                .filter(node -> node.getState() == NodeState.ALIVE)
                .map(NodeInfo::getId)
                .collect(Collectors.toList());

        // 3. rekonstriraj go ringot samo so ovie alive node id-a
        hashRing.rebuild(aliveNodeIds);

        System.out.println("Consistent Hash Ring rebuilt successfully with " + aliveNodeIds.size() + " ALIVE nodes.");
    }

    // vrakja momentalna slika za clusterot: nodeId -> NodeInfo
    public Map<String, NodeInfo> getClusterStatus() {
        return gossipService.getMembershipList();
    }

    // id na lokalniot node (host:port)
    public String getLocalNodeId() {
        return gossipService.getSelfId();
    }

    // gasi gossip servis i scheduler
    public void shutdown() {
        gossipService.shutdown();
    }

    // za daden key, vrakja koj node e odgovoren spored consistent hash ringot
    public String findResponsibleNode(String key) {
        return hashRing.getNode(key);
    }
}
