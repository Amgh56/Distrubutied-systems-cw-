import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileIndex {
    private final Set<String> storedFiles = ConcurrentHashMap.newKeySet();

    // Add filename after STORE_ACKs
    public void markAsStored(String filename) {
        storedFiles.add(filename);
    }

    // Check before accepting STORE request
    public boolean alreadyStored(String filename) {
        return storedFiles.contains(filename);
    }

    // For debugging or future use
    public Set<String> getAllStoredFiles() {
        return storedFiles;
    }
}
