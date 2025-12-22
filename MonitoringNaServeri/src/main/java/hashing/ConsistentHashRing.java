package hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

// klasa za consistent hash ring so virtualni jazli (replici)
public class ConsistentHashRing<T> {

    // podreden map: hash pozicija na prstenot -> node
    private final SortedMap<Long, T> ring = new TreeMap<>();
    // kolku virtualni jazli (replici) kreirame za sekoj node
    private final int numberOfReplicas;

    // konstruktor: postavuva broj na replici i gi dodava pocetnite jazli, ako gi ima
    public ConsistentHashRing(int numberOfReplicas, Collection<T> nodes) {
        this.numberOfReplicas = numberOfReplicas;
        if (nodes != null) {
            nodes.forEach(this::add);
        }
    }

    // kompletna rekonstrukcija na prstenot so nov set od jazli
    public synchronized void rebuild(Collection<T> newNodes) {
        ring.clear();
        if (newNodes != null) {
            newNodes.forEach(this::add);
        }
        System.out.println("[HashRing] Rebuilt. New node count: " +
                (newNodes != null ? newNodes.size() : 0));
    }

    // dodava node vo ringot so numberOfReplicas virtualni pozicii
    public synchronized void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = hash(node.toString() + "#" + i);
            ring.put(hash, node);
        }
    }

    // gi brise site virtualni pozicii sto pripagaat na daden node
    public synchronized void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = hash(node.toString() + "#" + i);
            ring.remove(hash);
        }
    }

    // za daden key go vrakja node-ot sto e sledna pozicija na prstenot
    public synchronized T getNode(Object key) {
        if (ring.isEmpty() || key == null) {
            return null;
        }

        long hash = hash(key.toString());
        // del od mapata so klucevi >= hash na keyot
        SortedMap<Long, T> tailMap = ring.tailMap(hash);

        // ako nema pobar velik hash, odi na prviot element (wrap okolu ringot)
        long nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        return ring.get(nodeHash);
    }

    // md5 baziran hash sto vrakja nenegativen long za pozicija na prstenot
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(key.getBytes());

            long value = ((long) (bytes[3] & 0xFF) << 24)
                    | ((long) (bytes[2] & 0xFF) << 16)
                    | ((long) (bytes[1] & 0xFF) << 8)
                    | (bytes[0] & 0xFF);

            // maskiranje za da dobieme samo nenegativna 32-bitna vrednost
            return value & 0xffffffffL;

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 hash algorithm not available", e);
        }
    }
}
