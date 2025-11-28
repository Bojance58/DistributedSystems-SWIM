package hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing<T> {

    private final SortedMap<Long, T> ring = new TreeMap<>();
    private final int numberOfReplicas;

    public ConsistentHashRing(int numberOfReplicas, Collection<T> nodes) {
        this.numberOfReplicas = numberOfReplicas;
        nodes.forEach(this::add);
    }

    /**
     * Реконструкција на прстенот со нови јазли.
     * Ги брише сите постоечки виртуелни јазли и ги додава новите.
     */
    public synchronized void rebuild(Collection<T> newNodes) {
        ring.clear();
        newNodes.forEach(this::add);
        System.out.println("Hash Ring rebuilt. New node count: " + newNodes.size());
    }

    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = hash(node.toString() + i);
            ring.put(hash, node);
        }
    }

    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = hash(node.toString() + i);
            ring.remove(hash);
        }
    }

    public T getNode(Object key) {
        if (ring.isEmpty()) {
            return null;
        }

        long hash = hash(key.toString());

        SortedMap<Long, T> tailMap = ring.tailMap(hash);

        long nodeHash;
        if (tailMap.isEmpty()) {
            nodeHash = ring.firstKey();
        } else {
            nodeHash = tailMap.firstKey();
        }

        return ring.get(nodeHash);
    }

    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(key.getBytes());
            // Користиме само првите 4 бајти за долг (long) хаш
            return ((long) (bytes[3] & 0xFF) << 24) |
                    ((long) (bytes[2] & 0xFF) << 16) |
                    ((long) (bytes[1] & 0xFF) << 8) |
                    (bytes[0] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 Hash не е достапен", e);
        }
    }
}