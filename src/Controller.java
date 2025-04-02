import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {
static Map<String, String> fileStatus = new ConcurrentHashMap<>();
static Map<String, List<Integer>> fileToDstores = new ConcurrentHashMap<>();
static Map<Integer, Integer> dstoreFileCounts = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        // Step 1: Parse command-line arguments
        int cport = Integer.parseInt(args[0]);           
        int R = Integer.parseInt(args[1]);               
        int timeout = Integer.parseInt(args[2]);        
        int rebalancePeriod = Integer.parseInt(args[3]);

        ServerSocket serverSocket = new ServerSocket(cport);
        System.out.println("[Controller] Listening on port " + cport);

        List<Integer> dstorePorts = new ArrayList<>();

        while (dstorePorts.size() < R) {
            Socket socket = serverSocket.accept();
            System.out.println("[Controller] Connection accepted");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String msg = in.readLine();
            System.out.println("[Controller] Received: " + msg);

            if (msg != null && msg.startsWith(Protocol.JOIN_TOKEN)) {
                String[] parts = msg.split(" ");
                if (parts.length == 2) {
                    int port = Integer.parseInt(parts[1]);
                    dstorePorts.add(port);
                    dstoreFileCounts.put(port, 0)
                    System.out.println("[Controller] Dstore joined on port: " + port);
                    System.out.println("[Controller] Total Dstores joined: " + dstorePorts.size());
                } else {
                    System.out.println("[Controller] Invalid JOIN message: " + msg);
                }
            }
        }

                while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("[Controller] Client connected");

            new Thread(() -> handleClient(clientSocket, dstorePorts, R)).start();
}

    }

private static void handleClient(Socket socket, List<Integer> dstorePorts, int R) {
    try (
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
    ) {
        String msg = in.readLine();
        System.out.println("[Controller] Received from client: " + msg);

        if (msg != null && msg.startsWith(Protocol.STORE_TOKEN)) {
            String[] parts = msg.split(" ");
            if (parts.length == 3) {
                String filename = parts[1];
                int filesize = Integer.parseInt(parts[2]);

                // 1. Check: Enough Dstores?
                if (dstorePorts.size() < R) {
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    return;
                }

                // 2. Check: File already exists?
                if (fileStatus.containsKey(filename)) {
                    out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return;
                }

                // 3. Mark as "IN_PROGRESS"
                fileStatus.put(filename, Protocol.STORE_IN_PROGRESS);

                // 4. Pick R Dstores (just take first R for now)
               // 4. Pick R least-loaded Dstores
                List<Integer> sortedDstores = new ArrayList<>(dstorePorts);
                sortedDstores.sort(Comparator.comparingInt(port -> dstoreFileCounts.getOrDefault(port, 0)));

                List<Integer> selectedDstores = sortedDstores.subList(0, R);

                // Update load counts
                for (int port : selectedDstores) {
                    dstoreFileCounts.put(port, dstoreFileCounts.getOrDefault(port, 0) + 1);
                }

                fileToDstores.put(filename, selectedDstores);


                // 5. Send STORE_TO message
                StringBuilder response = new StringBuilder(Protocol.STORE_TO_TOKEN);
                for (int port : selectedDstores) {
                    response.append(" ").append(port);
                }

                out.println(response.toString());
                System.out.println("[Controller] Sent to client: " + response);
            }
        }

    } catch (Exception e) {
        System.out.println("[Controller] Client error: " + e.getMessage());
    }
}


}

        