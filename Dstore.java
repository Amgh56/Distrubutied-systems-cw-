import java.io.*;
import java.net.*;

public class Dstore {
    public static void main(String[] args) throws Exception {
        // Step 1: Parse arguments
        int port = Integer.parseInt(args[0]);      // Dstore's own port (just for info)
        int cport = Integer.parseInt(args[1]);     // Controller's port
        int timeout = Integer.parseInt(args[2]);   // Not used yet
        String fileFolder = args[3];               // Not used yet

        // Step 2: Connect to the Controller
        Socket socket = new Socket("localhost", cport);
        System.out.println("[Dstore] Connected to Controller on port " + cport);

        // Step 3: Send JOIN message
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println(Protocol.JOIN_TOKEN +  " "  + port);  // e.g., JOIN 2000

        System.out.println("[Dstore] Sent JOIN message: JOIN " + port);
        
        // Step 4: Done (you can leave the socket open for future use)
        // Later you’ll keep this connection alive for STORE_ACK, REMOVE_ACK, etc.
    }
}
