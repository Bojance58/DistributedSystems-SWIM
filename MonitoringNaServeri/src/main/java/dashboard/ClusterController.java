package dashboard;

import gossip.GossipManager;
import gossip.NodeInfo;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

// rest kontroler sto nudi http api za pregled i kontrola na clusterot
@RestController
@RequestMapping("/cluster")
public class ClusterController {

    // referenca do gossip slojot sto ja znae sostojbata na site jazli
    private final GossipManager gossipManager;

    // constructor injection na GossipManager bean-ot od spring
    public ClusterController(GossipManager gossipManager) {
        this.gossipManager = gossipManager;
    }

    // get /cluster/status -> ja vrakja momentalnata membership mapa nodeId -> NodeInfo
    @GetMapping("/status")
    public Map<String, NodeInfo> getClusterStatus() {
        return gossipManager.getClusterStatus();
    }

    // get /cluster/rebalance -> racho ja povikuva logikata za rebuild na hash ring samo so alive jazli
    @GetMapping("/rebalance")
    public String rebalanceCluster() {
        gossipManager.rebuildHashRing();
        return "Hash Ring rebuilt successfully using ALIVE nodes.";
    }

    // get /cluster/me -> informativen endpoint koj kazuva koj e lokalniot node (host:port)
    @GetMapping("/me")
    public String getLocalNodeId() {
        return gossipManager.getLocalNodeId();
    }

    // get /cluster/find/{key} -> prasuva vo hash ring koj node treba da go obraboti daden key
    @GetMapping("/find/{key}")
    public String findResponsibleNode(@PathVariable String key) {
        String responsibleNode = gossipManager.findResponsibleNode(key);
        return String.format("Key '%s' is assigned to node: %s", key, responsibleNode);
    }
}
