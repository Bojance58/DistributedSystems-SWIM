package gossip;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Gossip порака што се праќа преку UDP (JSON).
 * Поддржува:
 *  - GOSSIP_PUSH: праќа дел/цел membership + digest
 *  - GOSSIP_PULL_REQ: бара листа на node-ови (digest = id -> dummy value)
 *  - GOSSIP_PULL_RES: враќа бараните NodeInfo во updates
 */
public class GossipMessage {

    private final String senderId;
    private final String type; // GOSSIP_PUSH / GOSSIP_PULL_REQ / GOSSIP_PULL_RES
    private final Map<String, NodeInfo> updates; // вистински записи (за PUSH и PULL_RES)
    private final Map<String, Long> digest;      // nodeId -> heartbeat (за PUSH) или само листа id-ја (за PULL_REQ)

    @JsonCreator
    public GossipMessage(@JsonProperty("senderId") String senderId,
                         @JsonProperty("type") String type,
                         @JsonProperty("updates") Map<String, NodeInfo> updates,
                         @JsonProperty("digest") Map<String, Long> digest) {
        this.senderId = senderId;
        this.type = type;
        this.updates = updates;
        this.digest = digest;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getType() {
        return type;
    }

    public Map<String, NodeInfo> getUpdates() {
        return updates;
    }

    public Map<String, Long> getDigest() {
        return digest;
    }
}
