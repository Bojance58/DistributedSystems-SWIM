package gossip;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
// zapis
public class NodeInfo {

    private final String id;
    private long heartbeat;
    private NodeState state;
    private long timestamp;

    @JsonCreator
    public NodeInfo(@JsonProperty("id") String id,
                    @JsonProperty("heartbeat") long heartbeat,
                    @JsonProperty("state") NodeState state,
                    @JsonProperty("timestamp") long timestamp) {
        this.id = id;
        this.heartbeat = heartbeat;
        this.state = state;
        this.timestamp = timestamp;
    }

    public NodeInfo() {
        this.id = null;
    }

    public String getId() {
        return id;
    }

    public synchronized long getHeartbeat() {
        return heartbeat;
    }

    public synchronized void incrementHeartbeat() {
        this.heartbeat++;
    }

    public synchronized void setHeartbeat(long heartbeat) {
        this.heartbeat = heartbeat;
    }

    public synchronized NodeState getState() {
        return state;
    }

    public synchronized void setState(NodeState state) {
        this.state = state;
    }

    public synchronized long getTimestamp() {
        return timestamp;
    }

    public synchronized void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "id='" + id + '\'' +
                ", heartbeat=" + heartbeat +
                ", state=" + state +
                ", timestamp=" + timestamp +
                '}';
    }
}
