package gossip;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

// udp implementacija na swim/gossip protokol za membership i failure detection
public class UDPGossipService {

    // maksimalna golemina na udp paket (bafer)
    private static final int UDP_BUFFER_SIZE = 65535;
    // interval pomegju dve gossip rundi vo ms
    private static final int GOSSIP_INTERVAL_MS = 3000;
    // kolku vreme sme spremni da cekame pred da proglasime SUSPECT
    private static final int SUSPECT_TIMEOUT_MS = 7000;
    // kolku vreme posle suspect cekame pred da proglasime DEAD
    private static final int DEAD_TIMEOUT_MS = 14000;
    // timeout za socket.receive za da ne blokira beskonecno
    private static final int SOCKET_TIMEOUT_MS = 2000;

    // tipovi na gossip poraki
    private static final String MSG_TYPE_GOSSIP_PUSH = "GOSSIP_PUSH";
    private static final String MSG_TYPE_GOSSIP_PULL_REQ = "GOSSIP_PULL_REQ";
    private static final String MSG_TYPE_GOSSIP_PULL_RES = "GOSSIP_PULL_RES";

    // jackson mapper za json (serijalizacija/de-serializacija)
    private final ObjectMapper mapper = new ObjectMapper();
    // id na ovaj node vo format host:port
    private final String selfId;
    // udp socket na koj slusame i prakjame gossip
    private final DatagramSocket socket;
    // lokalna membership mapa: nodeId -> NodeInfo
    private final Map<String, NodeInfo> membershipList;
    // scheduler za periodichni gossip i failure detection taskovi
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    // random generator za izbor na peer
    private final Random random = new Random();

    // konstruktor: otvara udp socket, gi dodava self i seed jazlite vo membership listata
    public UDPGossipService(String host, int port, List<String> seedNodes) throws SocketException {
        this.selfId = host + ":" + port;
        this.socket = new DatagramSocket(port);
        this.socket.setSoTimeout(SOCKET_TIMEOUT_MS);
        this.membershipList = new ConcurrentHashMap<>();

        // dodaj ja informacijata za sopstveniot node
        NodeInfo selfInfo = new NodeInfo(selfId, 0, NodeState.ALIVE, System.currentTimeMillis());
        membershipList.put(selfId, selfInfo);

        // dodavanje na seed nodes kako pocetno poznati jazli
        if (seedNodes != null) {
            for (String seed : seedNodes) {
                if (!seed.equals(selfId)) {
                    membershipList.put(seed, new NodeInfo(seed, 0, NodeState.ALIVE, 0));
                }
            }
        }
    }

    // startuvanje na receive nitka + periodichni gossip i failure detection taskovi
    public void start() {
        System.out.println("[Gossip] Service started on: " + selfId);

        // nitka koja neprekidno primase udp poraki
        Thread receiverThread = new Thread(this::receiveLoop, "udp-gossip-receiver-" + selfId);
        receiverThread.setDaemon(true);
        receiverThread.start();

        // periodicen gossip (push) kon slucaen peer
        scheduler.scheduleAtFixedRate(this::gossipLoop,
                0, GOSSIP_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // periodicen local failure detection baziran na timestamp
        scheduler.scheduleAtFixedRate(this::checkForFailures,
                GOSSIP_INTERVAL_MS, GOSSIP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // glavna nitka sto slusa udp poraki i gi delegira na handleri
    private void receiveLoop() {
        byte[] buffer = new byte[UDP_BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        while (!Thread.currentThread().isInterrupted() && !socket.isClosed()) {
            try {
                // blokira do SOCKET_TIMEOUT_MS ili do pristignuvanje na paket
                socket.receive(packet);
                String json = new String(packet.getData(), 0, packet.getLength());

                // ja citame GossipMessage od json
                GossipMessage message = mapper.readValue(json, GossipMessage.class);
                String type = message.getType();

                // prefrli na soodvetniot handler spored tipot na porakata
                if (MSG_TYPE_GOSSIP_PUSH.equals(type)) {
                    handleGossipPush(message);
                } else if (MSG_TYPE_GOSSIP_PULL_REQ.equals(type)) {
                    handleGossipPullReq(message);
                } else if (MSG_TYPE_GOSSIP_PULL_RES.equals(type)) {
                    handleGossipPullRes(message);
                }

            } catch (SocketTimeoutException ignored) {
                // normalno: isto vreme koristi se za da mozeme da proverime dali socketot e zatvoren
            } catch (SocketException se) {
                // ako socketot e zatvoren prekini ja receiveLoop
                if (socket.isClosed()) {
                    break;
                }
            } catch (Exception e) {
                System.err.println(selfId + ": Error while receiving gossip message: " + e.getMessage());
            }
        }
    }

    // obrabotka na GOSSIP_PUSH poraka
    private void handleGossipPush(GossipMessage message) {
        // 1) merge na dobienite updates vo lokalnata membership lista (anti-entropy)
        if (message.getUpdates() != null) {
            mergeMembershipList(message.getUpdates());
        }

        // 2) digest: proveruvame za koi node-ovi peer-ot ima pogolemi heartbeat-i
        Map<String, Long> remoteDigest = message.getDigest();
        if (remoteDigest == null || remoteDigest.isEmpty()) {
            return;
        }

        // soberi lista od id-a za koi ni nedostavaat ili zaostanuvame so versija
        List<String> missingIds = remoteDigest.entrySet().stream()
                .filter(e -> {
                    NodeInfo local = membershipList.get(e.getKey());
                    return local == null || local.getHeartbeat() < e.getValue();
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // ako ima nekoj zaostanat zapis, pobaraj gi so PULL_REQ
        if (!missingIds.isEmpty()) {
            sendPullRequest(message.getSenderId(), missingIds);
        }
    }

    // obrabotka na GOSSIP_PULL_REQ: peer bara da mu gi vratime odredeni NodeInfo
    private void handleGossipPullReq(GossipMessage message) {
        Map<String, Long> requested = message.getDigest();
        if (requested == null || requested.isEmpty()) {
            return;
        }

        // kreiraj mapa so trazhenite node-ovi so nivnite celosni NodeInfo podatoci
        Map<String, NodeInfo> updates = new HashMap<>();
        for (String id : requested.keySet()) {
            NodeInfo info = membershipList.get(id);
            if (info != null) {
                updates.put(id, info);
            }
        }

        // ako ima sto da se prati, vrati PULL_RES kon isprakacot
        if (!updates.isEmpty()) {
            sendPullResponse(message.getSenderId(), updates);
        }
    }

    // obrabotka na GOSSIP_PULL_RES: dobivame zaostanati/novi NodeInfo i gi mergirame
    private void handleGossipPullRes(GossipMessage message) {
        if (message.getUpdates() != null) {
            mergeMembershipList(message.getUpdates());
        }
    }

    // periodicen gossip ciklus: update na self i push kon slucaen peer
    private void gossipLoop() {
        try {
            // osvezi lokalniot node so nov heartbeat i timestamp
            NodeInfo self = membershipList.get(selfId);
            if (self != null) {
                self.incrementHeartbeat();
                self.setTimestamp(System.currentTimeMillis());
            }

            // odberi slucaen peer koj ne e DEAD i ne e self
            Optional<NodeInfo> peerOpt = selectRandomPeer();
            if (peerOpt.isEmpty()) {
                return;
            }

            NodeInfo peer = peerOpt.get();

            // vo ova poednostaveno scenario prakjame cela membership lista kako "diff"
            Map<String, NodeInfo> updates = new HashMap<>(membershipList);
            Map<String, Long> digest = buildDigest();

            GossipMessage message = new GossipMessage(
                    selfId,
                    MSG_TYPE_GOSSIP_PUSH,
                    updates,
                    digest
            );

            String json = mapper.writeValueAsString(message);
            sendUdpMessage(peer.getId(), json);

        } catch (Exception e) {
            System.err.println(selfId + ": Error in gossipLoop: " + e.getMessage());
        }
    }

    // gradi digest mapa: nodeId -> heartbeat za celata membership lista
    private Map<String, Long> buildDigest() {
        Map<String, Long> digest = new HashMap<>();
        membershipList.forEach((id, info) -> digest.put(id, info.getHeartbeat()));
        return digest;
    }

    // bira slucaen peer od dostupnite (alive ili suspect) jazli
    private Optional<NodeInfo> selectRandomPeer() {
        List<NodeInfo> availablePeers = membershipList.values().stream()
                .filter(n -> n.getState() != NodeState.DEAD && !n.getId().equals(selfId))
                .collect(Collectors.toList());

        if (availablePeers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(availablePeers.get(random.nextInt(availablePeers.size())));
    }


    // isprakja GOSSIP_PULL_REQ do target node so lista od ids za koi ni treba update
    private void sendPullRequest(String targetId, List<String> missingIds) {
        try {
            Map<String, Long> requestMap = new HashMap<>();
            for (String id : missingIds) {
                requestMap.put(id, 0L); // vrednosta ne e bitna, ni treba samo listata od klucevi
            }

            GossipMessage pullReq = new GossipMessage(
                    selfId,
                    MSG_TYPE_GOSSIP_PULL_REQ,
                    Collections.emptyMap(),
                    requestMap
            );

            String json = mapper.writeValueAsString(pullReq);
            sendUdpMessage(targetId, json);

        } catch (Exception e) {
            System.err.println(selfId + ": Error sending PULL_REQ: " + e.getMessage());
        }
    }

    // isprakja GOSSIP_PULL_RES so realnite NodeInfo updates kon baraniot node
    private void sendPullResponse(String targetId, Map<String, NodeInfo> updates) {
        try {
            GossipMessage pullRes = new GossipMessage(
                    selfId,
                    MSG_TYPE_GOSSIP_PULL_RES,
                    updates,
                    Collections.emptyMap()
            );

            String json = mapper.writeValueAsString(pullRes);
            sendUdpMessage(targetId, json);

        } catch (Exception e) {
            System.err.println(selfId + ": Error sending PULL_RES: " + e.getMessage());
        }
    }

    // spoi remote membership mapa vo lokalnata membershipList koristejki heartbeat za verzioniranje
    private void mergeMembershipList(Map<String, NodeInfo> remoteUpdates) {
        remoteUpdates.forEach((id, remoteInfo) -> {
            NodeInfo localInfo = membershipList.get(id);

            // ako prvi pat go gledame ovoj node -> dodaj go
            if (localInfo == null) {
                membershipList.put(id, remoteInfo);
                System.out.println(selfId + ": New node discovered: " + id);
            }
            // ako remote ima pogolem heartbeat -> prifati go kako ponova informaciona verzija
            else if (remoteInfo.getHeartbeat() > localInfo.getHeartbeat()) {
                localInfo.setHeartbeat(remoteInfo.getHeartbeat());
                localInfo.setState(remoteInfo.getState());
                localInfo.setTimestamp(System.currentTimeMillis());
                System.out.println(selfId + ": Updated node: " + id +
                        ", State: " + localInfo.getState());
            }
        });
    }

    // lokalno failure detection bazirano na timestamp i timeouts
    private void checkForFailures() {
        long now = System.currentTimeMillis();
        List<NodeInfo> nodes = new ArrayList<>(membershipList.values());

        for (NodeInfo node : nodes) {
            // preskokni self i vekje DEAD jazli
            if (node.getId().equals(selfId) || node.getState() == NodeState.DEAD) {
                continue;
            }

            long diff = now - node.getTimestamp();

            // ako node e suspect i mnogu vreme nema promena -> proglasi DEAD
            if (node.getState() == NodeState.SUSPECT && diff > DEAD_TIMEOUT_MS) {
                node.setState(NodeState.DEAD);
                node.incrementHeartbeat();
                node.setTimestamp(now);
                System.out.println(selfId + ": Declared DEAD: " + node.getId() +
                        " after " + diff + " ms");
            }
            // ako node e alive a nema aktivnost do SUSPECT_TIMEOUT_MS -> premesti vo SUSPECT
            else if (node.getState() == NodeState.ALIVE && diff > SUSPECT_TIMEOUT_MS) {
                node.setState(NodeState.SUSPECT);
                node.incrementHeartbeat();
                node.setTimestamp(now);
                System.out.println(selfId + ": Declared SUSPECT: " + node.getId() +
                        " after " + diff + " ms");
            }
        }
    }
    // 5) Pomosni metodi

    // isprakja json payload do targetId (host:port) preko udp paket
    private void sendUdpMessage(String targetId, String jsonPayload) throws Exception {
        String[] parts = targetId.split(":");
        if (parts.length != 2) {
            return;
        }

        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);
        byte[] buffer = jsonPayload.getBytes();

        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, port);
        socket.send(packet);
    }

    // daje pristap do lokalnata membership mapa
    public Map<String, NodeInfo> getMembershipList() {
        return membershipList;
    }

    // vrakja id na ovoj node
    public String getSelfId() {
        return selfId;
    }

    // gasi scheduler i udp socketot
    public void shutdown() {
        scheduler.shutdownNow();
        socket.close();
        System.out.println(selfId + ": Gossip Service shut down.");
    }

    // pomosen metod za test: na sila menuva sostojba na nekoj node lokalno
    public void forceUpdateLocalState(String targetId, NodeState newState) {
        NodeInfo info = membershipList.get(targetId);
        if (info != null) {
            info.setState(newState);
            info.incrementHeartbeat();
            info.setTimestamp(System.currentTimeMillis());
            System.out.println(selfId + ": SIMULATION: Forcing local state of " + targetId +
                    " to " + newState);
        }
    }
}
