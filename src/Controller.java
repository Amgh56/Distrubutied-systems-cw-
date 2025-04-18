import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {
static Map<String, List<Integer>> fileToDstores = new ConcurrentHashMap<>();
static Map<String, Set<Integer>> storeAcks = new ConcurrentHashMap<>();
static Map<String, Set<Integer>> removeAcks = new ConcurrentHashMap<>();
static Map<String, Socket> fileToClient = new ConcurrentHashMap<>();
static Map<Socket, Integer> socketToDstorePort = new ConcurrentHashMap<>();
enum FileStatus {
    STORE_IN_PROGRESS,
    STORE_COMPLETE,
    REMOVE_IN_PROGRESS
}
static Map<String, FileStatus> fileStatus = new ConcurrentHashMap<>();
static Map<String, Integer> fileSizes = new ConcurrentHashMap<>();
static Map<Socket, Map<String, Set<Integer>>> clientLoadHistory = new ConcurrentHashMap<>();
static volatile boolean rebalanceRunning = false;
static ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();
static int replicationFactor;
static int timeout;
static Map<Integer, Set<String>> dstoreFiles = new ConcurrentHashMap<>();
static CountDownLatch listLatch;
static Map<Integer, List<String>> filesToRemove = new HashMap<>();
static Map<Integer, Map<String, List<Integer>>> filesToSend = new HashMap<>();
static Queue<Runnable> pendingClientRequests = new ConcurrentLinkedQueue<>();
static Queue<Runnable> pendingJoinRequests = new ConcurrentLinkedQueue<>();
static Set<Integer> rebalanceAcks = ConcurrentHashMap.newKeySet();


    public static void main(String[] args) throws Exception {
    int cport = Integer.parseInt(args[0]);           
    replicationFactor = Integer.parseInt(args[1]);               
    timeout = Integer.parseInt(args[2]);        
    int rebalancePeriod = Integer.parseInt(args[3]);
    //This socket except accepts connections from both dstore and client.
    ServerSocket serverSocket = new ServerSocket(cport);
    List<Integer> dstorePorts = new ArrayList<>();
    System.out.println("[Controller] Listening on port " + cport);
    startRebalanceThread(rebalancePeriod);
        System.out.println("[Controller] rebalance started " );



    //handle conections recieved
    while (true) {
        Socket socket = serverSocket.accept();

       if (rebalanceRunning) {
         Socket finalSocket = socket;
        new Thread(() -> {
         try {
            BufferedReader in = new BufferedReader(new InputStreamReader(finalSocket.getInputStream()));
            String msg = in.readLine();

            if (msg != null && msg.startsWith(Protocol.JOIN_TOKEN)) {
                pendingJoinRequests.add(() -> handleJoin(msg, finalSocket, dstorePorts));
                System.out.println("[Controller] Queued JOIN during rebalance");
            } else {
                pendingClientRequests.add(() -> handleClient(finalSocket, dstorePorts, replicationFactor, msg));
                System.out.println("[Controller] Queued client request during rebalance");
            }
        } catch (IOException e) {
            System.out.println("[Controller] Error reading from socket during rebalance: " + e.getMessage());
        }
    }).start();
        } else {
            // Normal processing
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String msg = in.readLine();

            if (msg != null && msg.startsWith(Protocol.JOIN_TOKEN)) {
                handleJoin(msg, socket, dstorePorts);
            } else if (msg != null) {
                new Thread(() -> handleClient(socket, dstorePorts, replicationFactor, msg)).start();
            } else {
                socket.close();
            }
        }

    }
}



/**
 * Marks rebalance as completed and start working with the queued requests
 */
private static void finishRebalance() {
    rebalanceRunning = false;
    while (!pendingJoinRequests.isEmpty()) {
        pendingJoinRequests.poll().run();
    }
    while (!pendingClientRequests.isEmpty()) {
        pendingClientRequests.poll().run();
    }
    System.out.println("[Controller] Finished rebalance. Processed queued requests.");
}


/**
 * This method handles the dstore joins
 * @param msg the message recieved from the dstore something like (JOIN 2000).
 * @param socket socket represents the connection to the dstore.
 * @param dstorePorts a list that track all active dstore ports.
 */
private static void handleJoin(String msg, Socket socket, List<Integer> dstorePorts) {
    String[] parts = msg.split(" ");
    if (parts.length == 2) {
        int port = Integer.parseInt(parts[1]);
        dstorePorts.add(port);
        socketToDstorePort.put(socket, port);
        new Thread(() -> listenToDstore(socket)).start();
        System.out.println("[Controller] Dstore joined on port: " + port);
        System.out.println("[Controller] Total Dstores joined: " + dstorePorts.size());

        if (dstorePorts.size() >= replicationFactor && !rebalanceRunning) {
            triggerRebalance("Join");
        }

    } else {
        System.out.println("[Controller] Invalid JOIN message: " + msg);
    }
}



/**
 * This method is responsible of the communication with the client to handle the operation requested from the client.
 * @param socket connected to the client.
 * @param dstorePorts list of all active dstore ports.
 * @param message first message received by the client 
 */
    private static void handleClient(Socket socket, List<Integer> dstorePorts, int R, String message) {
    try{
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        String msg = message;
        System.out.println("[Controller] Received from client (initial): " + msg);
        handleMessageFromClient(socket,msg, out, dstorePorts, R);

        while ((msg = in.readLine()) != null) {
        System.out.println("[Controller] Received from client: " + msg);
        handleMessageFromClient(socket, msg, out, dstorePorts, R);
        }

        }catch (Exception e) {
        System.out.println("[Controller] Client error: " + e.getMessage());

     }
     
}
    

/**
 * Handle sotre ack from dstore and also check if the controller recived enough ack.
 * @param socket connected to the dstore .
 * @param msg Store ack message recieved
 * @param R the replication factor for the file required.
 */
   private static void handleStoreAck(Socket socket, String msg, int R) {
    try {
        System.out.println("[Controller] Received: " + msg);

        String[] parts = msg.split(" ");
        if (parts.length != 2) {
            System.out.println("[Controller] wrong STORE_ACK format");
            return;
        }

        String filename = parts[1];
        Integer dstorePort = socketToDstorePort.get(socket);
        if (dstorePort == null) {
            System.out.println("[Controller] Unknown socket for STORE_ACK");
            return;
        }

        List<Integer> assignedDstores = fileToDstores.get(filename);
        if (assignedDstores == null || !assignedDstores.contains(dstorePort)) {
            System.out.println("[Controller] Dstore " + dstorePort + " not assigned for " + filename);
            return;
        }

        storeAcks.putIfAbsent(filename, ConcurrentHashMap.newKeySet());
        storeAcks.get(filename).add(dstorePort);
        System.out.println("[Controller] Added ACK from Dstore " + dstorePort + " for file " + filename);

        int ackCount = storeAcks.get(filename).size();
        System.out.println("[Controller] Ack count for " + filename + ": " + ackCount + " / " + R);

        if (ackCount >= R) {
            FileStatus currentStatus = fileStatus.get(filename);
            if (currentStatus != FileStatus.STORE_IN_PROGRESS) {
                System.out.println("[Controller] Skipping STORE_COMPLETE send — status already changed.");
                return;
            }

            fileStatus.put(filename, FileStatus.STORE_COMPLETE);
            System.out.println("fileStatus updated to COMPLETE for: " + filename);
            System.out.println("Current fileStatus: " + fileStatus);

            Socket clientSocket = fileToClient.get(filename);
            if (clientSocket != null && !clientSocket.isClosed()) {
                PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                System.out.println("[Controller] sent STORE_COMPLETE to client for: " + filename);
            } else {
                System.out.println("[Controller] No valid client socket for file: " + filename);
            }

            // Clean up
            fileToClient.remove(filename);
            storeAcks.remove(filename);
        }

    } catch (Exception e) {
        System.out.println("[Controller] Error handling STORE_ACK: " + e.getMessage());
        e.printStackTrace();
    }
}


/**
 * handles the rebalance complete sent via dstore
 * @param socket dstore socket that sent the ack
 */
private static void handleRebalanceAck(Socket socket) {
    Integer port = socketToDstorePort.get(socket);
    if (port == null) {
        System.out.println("[Controller] Received REBALANCE_COMPLETE from unknown Dstore");
        return;
    }

    rebalanceAcks.add(port);
    System.out.println("[Controller] Received REBALANCE_COMPLETE from Dstore " + port);
}


    /**
     *  this method handle store ack recieved 
     * @param socket the socket connected to the dstore 
     */
    private static void listenToDstore(Socket socket) {
    try {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String msg;
        while ((msg = in.readLine()) != null) {
            System.out.println("[Controller] Received from Dstore socket: " + msg);
            if (msg.startsWith(Protocol.STORE_ACK_TOKEN)) {
                handleStoreAck(socket, msg, replicationFactor ); 
            } else if (msg.startsWith(Protocol.REMOVE_ACK_TOKEN) || msg.startsWith(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) {
                 handleRemoveAck(socket, msg, replicationFactor);
             } else if (msg.startsWith(Protocol.LIST_TOKEN)) {
                 handleListResponse(socket, msg);
             }else if (msg.startsWith(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                handleRebalanceAck(socket);
            }
            else {
                System.out.println("[Controller] Unknown message from dstore socket: " + msg);
            }
        }
        }
     catch (IOException e) {
        System.out.println("[Controller] Connection to Dstore lost: " + e.getMessage());
    }
}


/**
 * This method is responsiible of handling and managing the STORE request from the client.
 * @param socket connected to client.
 * @param msg expected message recieved from the client
 * @param out client output stream to respond to 
 * @param dstorePorts list of currently active dstores 
 * @param R the required replication factor 
 */
private static void handleMessageFromClient(Socket socket, String msg, PrintWriter out, List<Integer> dstorePorts, int R) {
        if (msg == null) {
        System.out.println("Recieved nothing");
        return;
        }

        if (!msg.startsWith(Protocol.RELOAD_TOKEN)) {
            clientLoadHistory.remove(socket);
         System.out.println("[Controller]  Cleared load retry history for client: " + socket);

        }

    if (msg.startsWith(Protocol.STORE_TOKEN)) {
        String[] parts = msg.split(" ");
        if (parts.length == 3) {
            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            fileSizes.put(filename, filesize);

          
            if (dstorePorts.size() < R) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                System.out.println("Warning " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }

            FileStatus currentStatus = fileStatus.get(filename);
        if (currentStatus == FileStatus.STORE_IN_PROGRESS || currentStatus == FileStatus.STORE_COMPLETE) {
            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            System.out.println("[Controller]  File already exists: " + filename);
            return;
        }
            fileStatus.put(filename, FileStatus.STORE_IN_PROGRESS);
            fileToClient.put(filename, socket);

            List<Integer> selectedDstores = dstorePorts.subList(0, R);
            fileToDstores.put(filename, selectedDstores);

            StringBuilder response = new StringBuilder(Protocol.STORE_TO_TOKEN);
            for (int port : selectedDstores) {
                response.append(" ").append(port);
            }

            try {
            PrintWriter freshOut = new PrintWriter(socket.getOutputStream(), true);
            freshOut.println(response);
        } catch (IOException e) {
            System.out.println("[Controller] Failed to respond to client: " + e.getMessage());
        }
            // out.println(response.toString());
            System.out.println("[Controller] Sent to client: " + response);

            new Thread(() -> {
                try {
                    Thread.sleep(timeout);
                    Set<Integer> acks = storeAcks.get(filename);
                    FileStatus status = fileStatus.get(filename);

                    if ((acks == null || acks.size() < R) && status == FileStatus.STORE_IN_PROGRESS) {
                        System.out.println("[Controller]  Timeout not all ack received: " + filename);
                        fileStatus.remove(filename);
                        fileToDstores.remove(filename);
                        fileToClient.remove(filename);
                        storeAcks.remove(filename);
                    }
                } catch (InterruptedException e) {
                    System.out.println("[Controller] Timeout thread interrupted for: " + filename);
                }
            }).start();
        }

        }else if (msg.equals(Protocol.LIST_TOKEN)) {
        handleListRequest(out, dstorePorts, R);

        }else if(msg.startsWith(Protocol.LOAD_TOKEN)){
            handleLoadRequest( socket,msg, out, dstorePorts, R);

    } else if (msg.startsWith(Protocol.RELOAD_TOKEN)) {
    handleReloadRequest(socket, msg, out, dstorePorts, R);
} else if (msg.startsWith(Protocol.REMOVE_TOKEN)) {
    handleRemoveRequest(socket, msg, out, dstorePorts, R);
}


}

/**
 * Handles the LIST request from the client.
 * Sends back a list of all stored files in STORE_COMPLETE status.
 * @param out respond to the client server.
 * @param dstorePorts save the ports of the dstore files.
 * @param R the replication number .
 */
private static void handleListRequest(PrintWriter out, List<Integer> dstorePorts, int R) {
    if (dstorePorts.size() < R) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        System.out.println("[Controller]  Not enough Dstores to serve LIST request.");
        return;
    }

   List<String> sortedFiles = new ArrayList<>();

for (Map.Entry<String, FileStatus> entry : fileStatus.entrySet()) {
    if (entry.getValue() == FileStatus.STORE_COMPLETE) {
        sortedFiles.add(entry.getKey());
    }
}

Collections.sort(sortedFiles); // Sort alphabetically

StringBuilder response = new StringBuilder(Protocol.LIST_TOKEN);
for (String filename : sortedFiles) {
    response.append(" ").append(filename);
}
    out.println(response.toString());
    System.out.println("[Controller]  Sent LIST response to client: " + response);
}


/**
 * Handles a load request from the client
 * @param socket The clients socket.
 * @param msg the incomping msg from the client LOAD filename.
 * @param out The output stream to respond to the client.
 * @param dstorePorts List of currently active Dstore ports.
 * @param R Replication factor.
 */
private static void handleLoadRequest(Socket socket,String msg, PrintWriter out, List<Integer> dstorePorts, int R) {

    String[] parts = msg.split(" ");
    if (parts.length != 2) {
        System.out.println("[Controller] something went wrong: " + msg);
        return;
    }

        String filename = parts[1];
        System.out.println("[Controller] Load request recieved ");


         if (dstorePorts.size() < R) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        System.out.println("[Controller] Not enough Dstores for LOAD: " + filename);
        return;
          }

        System.out.println("[Controller]  Enough Dstores available (" + dstorePorts.size() + ")");

        FileStatus status = fileStatus.get(filename);
        if (status != FileStatus.STORE_COMPLETE || !fileToDstores.containsKey(filename)) {
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            System.out.println("[Controller] File does not exsist: " + filename);
            return;
        }

        System.out.println("[Controller] File exists: " + filename);

        List<Integer> dstoresWithFile = fileToDstores.get(filename);
        if (dstoresWithFile == null || dstoresWithFile.isEmpty()) {
            out.println(Protocol.ERROR_LOAD_TOKEN);
            System.out.println("[Controller] No such file in the dstore: " + filename);
            return;
        }
        System.out.println("[Controller] Dstores available for " + filename + ": " + dstoresWithFile);


        Integer filesize = fileSizes.get(filename);
        if (filesize == null) {
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            System.out.println("[Controller]  File size not tracked for: " + filename);
            return;
        }

    // Choose a Dstore (first one for now)
    int selectedPort = dstoresWithFile.get(0);

    clientLoadHistory
        .computeIfAbsent(socket, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(filename, k -> ConcurrentHashMap.newKeySet())
        .add(selectedPort);

    out.println(Protocol.LOAD_FROM_TOKEN + " " + selectedPort + " " + filesize);
    System.out.println("[Controller] Sent LOAD_FROM to client for " + filename +
                       " | port=" + selectedPort + ", size=" + filesize + " bytes");

}




  
/**
 * Handles RELOAD request from the client.
 * @param socket The client's socket used to track  history.
 * @param msg The RELOAD message.
 * @param out The output stream to send responses to the client.
 * @param dstorePorts List of all active Dstore ports .
 * @param R The required replication factor.
 */
private static void handleReloadRequest(Socket socket, String msg, PrintWriter out, List<Integer> dstorePorts, int R) {
    String[] parts = msg.split(" ");
    if (parts.length != 2) {
        System.out.println("[Controller]  Malformed RELOAD message: " + msg);
        return;
    }

    String filename = parts[1];
    System.out.println("[Controller]  RELOAD requested for file: " + filename);

    FileStatus status = fileStatus.get(filename);
    if (status != FileStatus.STORE_COMPLETE || !fileToDstores.containsKey(filename)) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
    }

    List<Integer> dstoresWithFile = fileToDstores.get(filename);
    if (dstoresWithFile == null || dstoresWithFile.isEmpty()) {
        out.println(Protocol.ERROR_LOAD_TOKEN);
        return;
    }

    Set<Integer> triedPorts = clientLoadHistory
        .computeIfAbsent(socket, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(filename, k -> ConcurrentHashMap.newKeySet());

    for (int port : dstoresWithFile) {
        if (!triedPorts.contains(port)) {
            triedPorts.add(port);
            int filesize = fileSizes.getOrDefault(filename, -1);
            if (filesize == -1) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            out.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + filesize);
            System.out.println("[Controller]  RELOAD selected port " + port + " for " + filename);
            return;
        }
    }

    out.println(Protocol.ERROR_LOAD_TOKEN);
    System.out.println("[Controller]  All Dstores failed for file: " + filename);
}


/**
 * Handles the remove request from the client
 * @param socket client socket sending request
 * @param msg Remove message
 * @param out the ouptut stream to respont to the client 
 * @param dstorePorts the list of active ports
 * @param R the replication factor 
 */
private static void handleRemoveRequest(Socket socket, String msg, PrintWriter out, List<Integer> dstorePorts, int R) {
    String[] parts = msg.split(" ");
    if (parts.length != 2) {
        System.out.println("[Controller] Invalid REMOVE message: " + msg);
        return;
    }

    String filename = parts[1];

    if (dstorePorts.size() < R) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        System.out.println("[Controller] Not enough Dstores for REMOVE.");
        return;
    }

    FileStatus status = fileStatus.get(filename);
    if (status != FileStatus.STORE_COMPLETE || !fileToDstores.containsKey(filename)) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        System.out.println("[Controller] File does not exist or not in correct status: " + filename);
        return;
    }

    fileStatus.put(filename, FileStatus.REMOVE_IN_PROGRESS);
    removeAcks.putIfAbsent(filename, ConcurrentHashMap.newKeySet());
    fileToClient.put(filename, socket);

    List<Integer> dstoresWithFile = fileToDstores.get(filename);
    for (int port : dstoresWithFile) {
        try {
            Socket dstoreSocket = null;
            for (Map.Entry<Socket, Integer> entry : socketToDstorePort.entrySet()) {
                if (entry.getValue() == port) {
                    dstoreSocket = entry.getKey();
                    break;
                }
            }

            if (dstoreSocket != null && !dstoreSocket.isClosed()) {
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                dstoreOut.println(Protocol.REMOVE_TOKEN + " " + filename);
                dstoreOut.flush();
                System.out.println("[Controller] Sent REMOVE " + filename + " to Dstore " + port);
            }

        } catch (IOException e) {
            System.out.println("[Controller] Failed to send REMOVE to Dstore " + port + ": " + e.getMessage());
        }
    }

    new Thread(() -> {
        try {
            Thread.sleep(timeout);
            Set<Integer> acks = removeAcks.get(filename);
            FileStatus currentStatus = fileStatus.get(filename);

            if ((acks == null || acks.size() < R) && currentStatus == FileStatus.REMOVE_IN_PROGRESS) {
                System.out.println("[Controller] Timeout waiting for REMOVE_ACKs for: " + filename);
            }
        } catch (InterruptedException e) {
            System.out.println("[Controller] Timeout thread interrupted for REMOVE: " + filename);
        }
    }).start();
}


/**
 * Handles the remove ack recieved from the Dstore 
 * and sends the Remove complete to the client 
 * @param socket connected to the dstore sending the ack
 * @param msg Remove_ack message recieved 
 * @param R replication factor 
 */
private static void handleRemoveAck(Socket socket, String msg, int R) {
    try {
        System.out.println("[Controller] Received: " + msg);

        String[] parts = msg.split(" ");
        if (parts.length != 2) {
            System.out.println("[Controller] Malformed REMOVE_ACK or ERROR_FILE_DOES_NOT_EXIST: " + msg);
            return;
        }

        String filename = parts[1];
        Integer dstorePort = socketToDstorePort.get(socket);
        if (dstorePort == null) {
            System.out.println("[Controller] Unknown socket for REMOVE_ACK");
            return;
        }

        List<Integer> assignedDstores = fileToDstores.get(filename);
        if (assignedDstores == null || !assignedDstores.contains(dstorePort)) {
            System.out.println("[Controller] Dstore " + dstorePort + " not assigned for file: " + filename);
            return;
        }

         synchronized (filename.intern()) {
        removeAcks.computeIfAbsent(filename, k -> ConcurrentHashMap.newKeySet()).add(dstorePort);

        System.out.println("[Controller] Added REMOVE_ACK from Dstore " + dstorePort + " for file " + filename);

        Set<Integer> acks = removeAcks.get(filename);
        if (acks == null) {
            System.out.println("[Controller] Warning: Received duplicate REMOVE_ACK after completion for file: " + filename);
            return;
        }
        int ackCount = acks.size();

        System.out.println("[Controller] Ack count for REMOVE " + filename + ": " + ackCount + " / " + R);

        if (ackCount >= R) {
            System.out.println("[Controller] All REMOVE_ACKs received for: " + filename);

            // Remove file from all relevant maps
            fileStatus.remove(filename);
            fileToDstores.remove(filename);
            fileSizes.remove(filename);
            removeAcks.remove(filename);

            // Send REMOVE_COMPLETE to client
            Socket clientSocket = fileToClient.get(filename);
            if (clientSocket != null && !clientSocket.isClosed()) {
                PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                clientOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                clientOut.flush();
                System.out.println("[Controller] Sent REMOVE_COMPLETE to client for: " + filename);
            }

            fileToClient.remove(filename); // cleanup
        }
    }

    } catch (Exception e) {
        System.out.println("[Controller] Error handling REMOVE_ACK: " + e.getMessage());
        e.printStackTrace();
    }
}


/**
 * Starts the background thread for the Rebalance operation
 * @param rebalancePeriod the time interval in seconds
 */
private static void startRebalanceThread(int rebalancePeriod) {
    rebalanceScheduler.scheduleAtFixedRate(() -> {
        if (rebalanceRunning) {
            System.out.println("[Controller] Rebalance already running — skipping this period.");
            return;
        }
        triggerRebalance("Periodic");
    }, rebalancePeriod, rebalancePeriod, TimeUnit.SECONDS);
}


/**
 * This method triggers a rebalance to ensure files are distirbuted correctly through dstores
 * @param reason of the trigger either periodically or if a new JOIN triggered
 */
private static void triggerRebalance(String reason) {
    if (!canTriggerRebalance(reason)) return;

    rebalanceRunning = true;
    System.out.println("[Controller]  Rebalance triggered: " + reason);

    requestDstoreFileLists();

    new Thread(() -> {
        try {
            waitForListResponses();
            Map<String, Set<Integer>> fileToActualDstores = computeFileToActualDstores();
            computeFilesToSendAndRemove(fileToActualDstores);
            balanceFileDistribution(fileToActualDstores);
            sendRebalanceInstructions();
            waitForRebalanceAcks();
            cleanupIndexIfNeeded(fileToActualDstores);
        } catch (InterruptedException e) {
            System.out.println("[Controller] Rebalance thread interrupted.");
        } finally {
            rebalanceRunning = false;
            finishRebalance();
        }
    }).start();
}

/**
 * @param reason of the trigger either periodically or if a new JOIN triggered
 * @return true if a trigger is allowed
 */
private static boolean canTriggerRebalance(String reason) {
    if (rebalanceRunning) {
        System.out.println("[Controller] Rebalance already running. Skipping: " + reason);
        return false;
    }
    if (socketToDstorePort.size() < replicationFactor) {
        System.out.println("[Controller] Not enough Dstores to trigger rebalance (" + socketToDstorePort.size() + "/" + replicationFactor + ")");
        return false;
    }
    for (FileStatus status : fileStatus.values()) {
        if (status == FileStatus.STORE_IN_PROGRESS || status == FileStatus.REMOVE_IN_PROGRESS) {
            System.out.println("[Controller] Rebalance postponed due to pending STORE or REMOVE");
            return false;
        }
    }
    return true;
}

/**
 * this method waits for all dstore to respond to the LIST request 
 */
private static void waitForListResponses() throws InterruptedException {
    if (!listLatch.await(timeout, TimeUnit.MILLISECONDS)) {
        System.out.println("[Controller] Timeout waiting for LIST responses.");
    } else {
        System.out.println("[Controller] All LIST responses received.");
    }
}

/**
 * this method builds a map based on the files inside eact dstore 
 */
private static Map<String, Set<Integer>> computeFileToActualDstores() {
    Map<String, Set<Integer>> result = new TreeMap<>();
    for (Map.Entry<Integer, Set<String>> entry : dstoreFiles.entrySet()) {
        for (String filename : entry.getValue()) {
            result.computeIfAbsent(filename, k -> new TreeSet<>()).add(entry.getKey());
        }
    }
    System.out.println("[Rebalance] Built fileToActualDstores: " + result);
    return result;
}


/**
 * This method is responsible about computing which files need to be removed or sent to anotehr dstore 
 * @param fileToActualDstores a map of file names and a set of the dstore storing them 
 */
private static void computeFilesToSendAndRemove(Map<String, Set<Integer>> fileToActualDstores) {
    filesToSend.clear();
    filesToRemove.clear();

    for (Map.Entry<String, Set<Integer>> entry : fileToActualDstores.entrySet()) {
        String filename = entry.getKey();
        Set<Integer> currentDstores = new HashSet<>(entry.getValue());

        if (!fileStatus.containsKey(filename)) {
            for (int port : currentDstores) {
                filesToRemove.computeIfAbsent(port, k -> new ArrayList<>()).add(filename);
            }
            continue;
        }

        if (currentDstores.size() < replicationFactor) {
            Set<Integer> neededDstores = new HashSet<>(socketToDstorePort.values());
            neededDstores.removeAll(currentDstores);
            Iterator<Integer> iter = neededDstores.iterator();

            while (currentDstores.size() < replicationFactor && iter.hasNext()) {
                int newTarget = iter.next();
                int source = currentDstores.iterator().next();

                filesToSend.computeIfAbsent(source, k -> new TreeMap<>())
                    .computeIfAbsent(filename, k -> new ArrayList<>()).add(newTarget);

                currentDstores.add(newTarget);
            }
        }

        if (currentDstores.size() > replicationFactor) {
            List<Integer> sortedDstores = new ArrayList<>(currentDstores);
            Collections.sort(sortedDstores);
            while (sortedDstores.size() > replicationFactor) {
                int excess = sortedDstores.remove(sortedDstores.size() - 1);
                filesToRemove.computeIfAbsent(excess, k -> new ArrayList<>()).add(filename);
            }
        }
    }

    for (Map.Entry<Integer, Set<String>> entry : dstoreFiles.entrySet()) {
        for (String file : entry.getValue()) {
            if (!fileToActualDstores.containsKey(file)) {
                filesToRemove.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(file);
            }
        }
    }

    System.out.println("[Rebalance] Files to remove: " + filesToRemove);
    System.out.println("[Rebalance] Files to send: " + filesToSend);
}


/**
 * balances the each dstore to ensure each dstore have a fair number of files stored
 * @param fileToActualDstores a map of file names and a set of the dstore storing them 
 */
private static void balanceFileDistribution(Map<String, Set<Integer>> fileToActualDstores) {
    int F = fileStatus.size();
    int N = socketToDstorePort.size();
    int R = replicationFactor;
    int totalCopies = F * R;
    int targetMin = totalCopies / N;
    int targetMax = (int) Math.ceil((double) totalCopies / N);
    System.out.println("[Rebalance] Target files per Dstore: min = " + targetMin + ", max = " + targetMax);

    Map<Integer, Integer> dstoreToFileCount = new HashMap<>();
    Map<Integer, Set<String>> dstoreToFiles = new HashMap<>();

    for (Map.Entry<Integer, Set<String>> entry : dstoreFiles.entrySet()) {
        dstoreToFileCount.put(entry.getKey(), entry.getValue().size());
        dstoreToFiles.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    PriorityQueue<Integer> overloaded = new PriorityQueue<>((a, b) -> dstoreToFileCount.get(b) - dstoreToFileCount.get(a));
    PriorityQueue<Integer> underloaded = new PriorityQueue<>((a, b) -> dstoreToFileCount.get(a) - dstoreToFileCount.get(b));

    for (int dstore : dstoreToFileCount.keySet()) {
        int count = dstoreToFileCount.get(dstore);
        if (count > targetMax) overloaded.add(dstore);
        else if (count < targetMin) underloaded.add(dstore);
    }

    while (!overloaded.isEmpty() && !underloaded.isEmpty()) {
        int from = overloaded.poll();
        int to = underloaded.poll();

        for (String file : dstoreToFiles.get(from)) {
            Set<Integer> holders = fileToActualDstores.get(file);
            if (holders == null || holders.contains(to)) continue;

            filesToSend.computeIfAbsent(from, k -> new TreeMap<>())
                .computeIfAbsent(file, k -> new ArrayList<>()).add(to);

            filesToRemove.computeIfAbsent(from, k -> new ArrayList<>()).add(file);
            holders.add(to);
            dstoreToFileCount.put(from, dstoreToFileCount.get(from) - 1);
            dstoreToFileCount.put(to, dstoreToFileCount.get(to) + 1);
            dstoreToFiles.get(to).add(file);
            dstoreToFiles.get(from).remove(file);
            break;
        }

        if (dstoreToFileCount.get(from) > targetMax) overloaded.add(from);
        if (dstoreToFileCount.get(to) < targetMin) underloaded.add(to);
    }
}

/**
 * this method sends rebalance commands to each dstore
 */
private static void sendRebalanceInstructions() {
    for (int dstore : socketToDstorePort.values()) {
        StringBuilder msg = new StringBuilder(Protocol.REBALANCE_TOKEN);

        Map<String, List<Integer>> toSend = filesToSend.getOrDefault(dstore, new TreeMap<>());
        msg.append(" ").append(toSend.size());
        for (Map.Entry<String, List<Integer>> sendEntry : toSend.entrySet()) {
            msg.append(" ").append(sendEntry.getKey()).append(" ").append(sendEntry.getValue().size());
            for (int target : sendEntry.getValue()) msg.append(" ").append(target);
        }

        List<String> toRemove = filesToRemove.getOrDefault(dstore, new ArrayList<>());
        msg.append(" ").append(toRemove.size());
        for (String file : toRemove) msg.append(" ").append(file);

        try {
            for (Map.Entry<Socket, Integer> entry : socketToDstorePort.entrySet()) {
                if (entry.getValue() == dstore) {
                    PrintWriter out = new PrintWriter(entry.getKey().getOutputStream(), true);
                    out.println(msg);
                    System.out.println("[Controller] Sent REBALANCE to Dstore " + dstore + ": " + msg);
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("[Controller] Failed to send REBALANCE to Dstore " + dstore + ": " + e.getMessage());
        }
    }
}


/**
 * this method waits for rebalance complete ack until it recieved it all of time out.
 */
private static void waitForRebalanceAcks() {
    rebalanceAcks.clear();
    System.out.println("[Controller] Waiting for REBALANCE_COMPLETE from all Dstores...");
    long start = System.currentTimeMillis();
    while (rebalanceAcks.size() < socketToDstorePort.size()) {
        if (System.currentTimeMillis() - start > timeout) {
            System.out.println("[Controller] Timeout waiting for REBALANCE_COMPLETEs.");
            break;
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            break;
        }
    }
}

/**
 * removes a file from a dstore if not found in the controller index.
 */
private static void cleanupIndexIfNeeded(Map<String, Set<Integer>> fileToActualDstores) {
    for (String file : new ArrayList<>(fileStatus.keySet())) {
        if (!fileToActualDstores.containsKey(file)) {
            System.out.println("[Rebalance]  File " + file + " is in the index but not found on any Dstore. It should be removed from the index.");
            fileStatus.remove(file);
            fileToDstores.remove(file);
            fileSizes.remove(file);
        }
    }
}



/**
 * Sends a List command to all of the connected dstores.
 */
    private static void requestDstoreFileLists() {
    listLatch = new CountDownLatch(socketToDstorePort.size());
    dstoreFiles.clear();

    for (Socket socket : socketToDstorePort.keySet()) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(Protocol.LIST_TOKEN);
            System.out.println("[Controller] Sent LIST to Dstore on port " + socketToDstorePort.get(socket));
        } catch (IOException e) {
            System.out.println("[Controller] Failed to send LIST to Dstore: " + e.getMessage());
        }
    }
}


/**
 * Handles a List response recieved from the dstore by parsing the filenames
 * @param socket from where the list operation where recieved
 * @param msg the full list message recieved.
 */
private static void handleListResponse(Socket socket, String msg) {
    Integer port = socketToDstorePort.get(socket);
    if (port == null) {
        System.out.println("[Controller] LIST from unknown socket");
        return;
    }

    String[] parts = msg.split(" ");
    Set<String> files = new HashSet<>();
    for (int i = 1; i < parts.length; i++) {
        files.add(parts[i]);
    }

    dstoreFiles.put(port, files);
    System.out.println("[Controller] LIST from Dstore " + port + ": " + files);

    listLatch.countDown();  
}


}
    