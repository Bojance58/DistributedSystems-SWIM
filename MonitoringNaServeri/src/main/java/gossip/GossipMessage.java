package gossip;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GossipMessage {

    private final String senderId;
    private final Map<String, NodeInfo> updates; // "Дифот" на листата на членови
    private final String type;

    @JsonCreator
    public GossipMessage(@JsonProperty("senderId") String senderId,
                         @JsonProperty("updates") Map<String, NodeInfo> updates,
                         @JsonProperty("type") String type) {
        this.senderId = senderId;
        this.updates = updates;
        this.type = type;
    }

    // Getters
    public String getSenderId() { return senderId; }
    public Map<String, NodeInfo> getUpdates() { return updates; }
    public String getType() { return type; }
}
