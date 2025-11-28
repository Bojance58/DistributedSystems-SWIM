package gossip;

public enum NodeState {
    ALIVE,    // Активен и одговара
    SUSPECT,  // Се сомневаме дека е паднат
    DEAD      // Потврдено е дека е паднат
}