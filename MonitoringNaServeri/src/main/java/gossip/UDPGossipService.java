package gossip;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class UDPGossipService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String selfId;
    private final DatagramSocket socket;
    private final Map<String, NodeInfo> membershipList;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private static final int UDP_BUFFER_SIZE = 65535;

    // АЖУРИРАН ИНТЕРВАЛ: 3 секунди
    private static final int GOSSIP_INTERVAL_MS = 3000;

    // НОВИ КОНСТАНТИ за лесна визуелна проверка:
    private static final int SUSPECT_TIMEOUT_MS = 7000;
    private static final int DEAD_TIMEOUT_MS = 14000;

    public UDPGossipService(String host, int port, List<String> seedNodes) throws SocketException, UnknownHostException {
        this.selfId = host + ":" + port;
        this.socket = new DatagramSocket(port);
        this.membershipList = new ConcurrentHashMap<>();

        NodeInfo selfInfo = new NodeInfo(selfId, 0, NodeState.ALIVE, System.currentTimeMillis());
        membershipList.put(selfId, selfInfo);

        for (String seed : seedNodes) {
            if (!seed.equals(selfId)) {
                membershipList.put(seed, new NodeInfo(seed, 0, NodeState.ALIVE, 0));
            }
        }
    }

    public void start() {
        System.out.println("Gossip Service started on: " + selfId);

        new Thread(this::receiveLoop).start();

        // 1. Gossip Cycle
        scheduler.scheduleAtFixedRate(this::gossipLoop, 0, GOSSIP_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // 2. Failure Detection Cycle
        scheduler.scheduleAtFixedRate(this::checkForFailures, GOSSIP_INTERVAL_MS, GOSSIP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // --- FAILURE DETECTION LOGIC (SUSPECT/DEAD) ---
    private void checkForFailures() {
        long currentTime = System.currentTimeMillis();

        List<NodeInfo> nodesToCheck = new ArrayList<>(membershipList.values());

        for (NodeInfo node : nodesToCheck) {

            if (node.getId().equals(selfId) || node.getState() == NodeState.DEAD) {
                continue;
            }

            long timeSinceLastSeen = currentTime - node.getTimestamp();

            // LOGIC FOR DEAD
            if (node.getState() == NodeState.SUSPECT && timeSinceLastSeen > DEAD_TIMEOUT_MS) {
                node.setState(NodeState.DEAD);
                node.incrementHeartbeat();
                node.setTimestamp(currentTime);
                System.out.println(selfId + ": 💀 Declared DEAD: " + node.getId() + " after " + timeSinceLastSeen + "ms");

                // LOGIC FOR SUSPECT
            } else if (node.getState() == NodeState.ALIVE && timeSinceLastSeen > SUSPECT_TIMEOUT_MS) {
                node.setState(NodeState.SUSPECT);
                node.incrementHeartbeat();
                node.setTimestamp(currentTime);
                System.out.println(selfId + ": ⚠️ Declared SUSPECT: " + node.getId() + " after " + timeSinceLastSeen + "ms");
            }
        }
    }

    private void receiveLoop() {
        byte[] buffer = new byte[UDP_BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                socket.receive(packet);
                String json = new String(packet.getData(), 0, packet.getLength());
                GossipMessage message = mapper.readValue(json, GossipMessage.class);

                mergeMembershipList(message.getUpdates());

            } catch (SocketTimeoutException ignored) {
            } catch (Exception e) {
                // Ignore errors
            }
        }
    }

    private void gossipLoop() {
        try {
            NodeInfo self = membershipList.get(selfId);
            if (self != null) {
                self.incrementHeartbeat();
                self.setTimestamp(System.currentTimeMillis());
            }

            Optional<NodeInfo> peer = selectRandomPeer();
            if (peer.isEmpty()) return;

            GossipMessage message = new GossipMessage(selfId, new HashMap<>(membershipList), "GOSSIP_PUSH");
            String json = mapper.writeValueAsString(message);

            sendUdpMessage(peer.get().getId(), json);

        } catch (Exception e) {
            // Ignore
        }
    }

    private void sendUdpMessage(String targetId, String jsonPayload) throws Exception {
        String[] parts = targetId.split(":");
        if (parts.length != 2) return;

        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);
        byte[] buffer = jsonPayload.getBytes();

        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, port);
        socket.send(packet);
    }

    private void mergeMembershipList(Map<String, NodeInfo> remoteUpdates) {
        remoteUpdates.forEach((id, remoteInfo) -> {
            NodeInfo localInfo = membershipList.get(id);

            if (localInfo == null) {
                membershipList.put(id, remoteInfo);
                System.out.println(selfId + ": New node discovered: " + id);
            } else if (remoteInfo.getHeartbeat() > localInfo.getHeartbeat()) {

                // Accepting new SUSPECT/DEAD states
                localInfo.setHeartbeat(remoteInfo.getHeartbeat());
                localInfo.setState(remoteInfo.getState());
                localInfo.setTimestamp(System.currentTimeMillis());
                System.out.println(selfId + ": Updated node: " + id + ", State: " + localInfo.getState());
            }
        });
    }

    private Optional<NodeInfo> selectRandomPeer() {
        // Selects only ALIVE or SUSPECT nodes for communication.
        List<NodeInfo> availablePeers = membershipList.values().stream()
                .filter(n -> n.getState() != NodeState.DEAD && !n.getId().equals(selfId))
                .collect(Collectors.toList());

        if (availablePeers.isEmpty()) {
            return Optional.empty();
        }

        Random random = new Random();
        return Optional.of(availablePeers.get(random.nextInt(availablePeers.size())));
    }

    // ПОПРАВЕНО: non-static метод
    public Map<String, NodeInfo> getMembershipList() {
        return membershipList;
    }

    public void shutdown() {
        scheduler.shutdownNow();
        socket.close();
        System.out.println(selfId + ": Gossip Service shut down.");
    }

    public String getSelfId() {
        return selfId;
    }

    public void forceUpdateLocalState(String targetId, NodeState newState) {
        NodeInfo info = membershipList.get(targetId);
        if (info != null) {
            info.setState(newState);
            info.incrementHeartbeat();
            info.setTimestamp(System.currentTimeMillis());

            System.out.println(selfId + ": SIMULATION: Forcing local state of " + targetId + " to " + newState);
        }
    }
}