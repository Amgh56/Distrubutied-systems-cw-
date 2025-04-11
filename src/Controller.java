import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {
static Map<String, List<Integer>> fileToDstores = new ConcurrentHashMap<>();
static Map<String, Set<Integer>> storeAcks = new ConcurrentHashMap<>();
static Map<String, PrintWriter> fileToClient = new ConcurrentHashMap<>();
static Map<Socket, Integer> socketToDstorePort = new ConcurrentHashMap<>();
enum FileStatus {
    STORE_IN_PROGRESS,
    STORE_COMPLETE
}
static Map<String, FileStatus> fileStatus = new ConcurrentHashMap<>();
static Map<String, Integer> fileSizes = new ConcurrentHashMap<>();
static Map<Socket, Map<String, Set<Integer>>> clientLoadHistory = new ConcurrentHashMap<>();

static int replicationFactor;
static int timeout;


    public static void main(String[] args) throws Exception {
    int cport = Integer.parseInt(args[0]);           
    replicationFactor = Integer.parseInt(args[1]);               
    timeout = Integer.parseInt(args[2]);        
    int rebalancePeriod = Integer.parseInt(args[3]);
    //This socket except accepts connections from both dstore and client.
    ServerSocket serverSocket = new ServerSocket(cport);
    List<Integer> dstorePorts = new ArrayList<>();
    System.out.println("[Controller] Listening on port " + cport);

    //handle conections recieved
    while (true) {
        Socket socket = serverSocket.accept();
        System.out.println("[Controller] Connection accepted");

        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String msg = in.readLine();
        System.out.println("[Controller] Received: " + msg);

        if (msg != null && msg.startsWith(Protocol.JOIN_TOKEN)) {
            handleJoin(msg, socket, dstorePorts);
        } else if (msg != null) {
            new Thread(() -> handleClient(socket, dstorePorts, replicationFactor, msg)).start();
        }else {
            System.out.println("[Controller] Error things did not go will : " + msg);
            socket.close();
        }
    }
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

        System.out.println("[Controller] Ack count for " + filename + ": " + storeAcks.get(filename).size());
        System.out.println("[Controller] Checking if all ACKs received for " + filename + ": " + storeAcks.get(filename).size() + " / " + R);

        if (storeAcks.get(filename).size() >= R) {
            fileStatus.put(filename, FileStatus.STORE_COMPLETE);
            System.out.println("fileStatus updated to COMPLETE for: " + filename);
            System.out.println("Current fileStatus: " + fileStatus);

            PrintWriter clientOut = fileToClient.get(filename);
            if (clientOut != null) {
                clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                System.out.println("[Controller] sent STORE_COMPLETE to client for: " + filename);
                fileToClient.remove(filename);
                storeAcks.remove(filename);
            } else {
                System.out.println("[Controller] there is no client for this file: " + filename);
            }
        }
    
    } catch (Exception e) {
        System.out.println("[Controller] Error handling STORE_ACK: " + e.getMessage());
    }

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
            } else {
                System.out.println("[Controller] Unknown message from dstore socket: " + msg);
            }
        }
    } catch (IOException e) {
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
         System.out.println("[Controller] 🔄 Cleared load retry history for client: " + socket);

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
            fileToClient.put(filename, out);

            List<Integer> selectedDstores = dstorePorts.subList(0, R);
            fileToDstores.put(filename, selectedDstores);

            StringBuilder response = new StringBuilder(Protocol.STORE_TO_TOKEN);
            for (int port : selectedDstores) {
                response.append(" ").append(port);
            }

            out.println(response.toString());
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
}

}

/**
 * Handles the LIST request from the client.
 * Sends back a list of all stored files in STORE_COMPLETE status.
 * @param out
 * @param dstorePorts 
 * @param R 
 */
private static void handleListRequest(PrintWriter out, List<Integer> dstorePorts, int R) {
    // Step 1: Check if we have enough Dstores
    if (dstorePorts.size() < R) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        System.out.println("[Controller]  Not enough Dstores to serve LIST request.");
        return;
    }

    StringBuilder response = new StringBuilder(Protocol.LIST_TOKEN);
    for (Map.Entry<String, FileStatus> entry : fileStatus.entrySet()) {
        if (entry.getValue() == FileStatus.STORE_COMPLETE) {
            response.append(" ").append(entry.getKey());
        }
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




}
    