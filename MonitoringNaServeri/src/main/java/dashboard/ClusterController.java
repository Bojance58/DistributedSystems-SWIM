package dashboard;

import gossip.GossipManager;
import gossip.NodeInfo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/cluster")
public class ClusterController {

    private final GossipManager gossipManager;

    public ClusterController(GossipManager gossipManager) {
        this.gossipManager = gossipManager;
    }

    /**
     * GET /cluster/status
     */
    @GetMapping("/status")
    public Map<String, NodeInfo> getClusterStatus() {
        return gossipManager.getClusterStatus();
    }

    // --- NEW ENDPOINT FOR REBALANCING ---
    /**
     * GET /cluster/rebalance
     */
    @GetMapping("/rebalance")
    public String rebalanceCluster() {
        // This method must trigger the logic inside GossipManager to rebuild the hash ring
        gossipManager.rebuildHashRing();
        return "Hash Ring rebuilt successfully using ALIVE nodes.";
    }
    // ------------------------------------

    /**
     * GET /cluster/me
     */
    @GetMapping("/me")
    public String getLocalNodeId() {
        return gossipManager.getLocalNodeId();
    }

    /**
     * GET /cluster/find/{key}
     */
    @GetMapping("/find/{key}")
    public String findResponsibleNode(@PathVariable String key) {
        String responsibleNode = gossipManager.findResponsibleNode(key);
        return String.format("Key '%s' is assigned to node: %s", key, responsibleNode);
    }
}