import java.io.*;
import java.net.*;
import java.io.DataInputStream;

public class Dstore {
private static Socket controllerSocket;
private static PrintWriter controllerOut;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);      
        int cport = Integer.parseInt(args[1]);     
        int timeout = Integer.parseInt(args[2]);   
        String fileFolder = args[3];               
        
        if (!prepareFolder(fileFolder)) return;

        System.out.println("[Dstore] Folder ready and cleaned: " + fileFolder);

        controllerSocket= new Socket("localhost", cport);
        System.out.println("[Dstore] Connected to Controller on port " + cport);

        controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        controllerOut.println(Protocol.JOIN_TOKEN +  " "  + port);  
        new Thread(() -> handleControllerCommands(controllerSocket, fileFolder)).start();

        System.out.println("[Dstore] Sent JOIN message: JOIN " + port);

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[Dstore] Listening for clients on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("[Dstore] Client connected");

            new Thread(() -> handleClient(clientSocket, fileFolder, cport)).start();
        }

    }


/**
 * Handles a client store request by receiving the file contents and saving them and sending ACK to the controller 
 * @param socket the client socket connection
 * @param fileFolder the directory to save received files in
 * @param cport the controller's port
 */
    private static void handleClient(Socket socket, String fileFolder, int cport) {
    try (
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
    ) {
        System.out.println("[Dstore]  Waiting for message...");
        String msg = in.readLine();
        System.out.println("[Dstore] Received: " + msg);

        if (msg.startsWith(Protocol.STORE_TOKEN)) {
            String[] parts = msg.split(" ");
            if (parts.length == 3) {
                String filename = parts[1];
                int filesize = Integer.parseInt(parts[2]);

                System.out.println("[Dstore] Preparing to receive file: " + filename + " (" + filesize + " bytes)");
                out.println(Protocol.ACK_TOKEN);
                System.out.println("[Dstore] Sent: " + Protocol.ACK_TOKEN);

                File file = new File(fileFolder, filename);
                try (DataInputStream dataIn = new DataInputStream(socket.getInputStream());
                     FileOutputStream fos = new FileOutputStream(file)) {
                    byte[] buffer = new byte[filesize];
                    dataIn.readFully(buffer);
                    fos.write(buffer);
                    System.out.println("[Dstore] File saved: " + file.getAbsolutePath());
                }

                controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                System.out.println("[Dstore] Sent STORE_ACK to controller for: " + filename);
            }

        } else if (msg.startsWith(Protocol.LOAD_DATA_TOKEN)) {
            String[] parts = msg.split(" ");
            if (parts.length == 2) {
                String filename = parts[1];
                File file = new File(fileFolder, filename);

                if (file.exists()) {
                    System.out.println("[Dstore] Preparing to send file: " + filename);
                    try (BufferedOutputStream outData = new BufferedOutputStream(socket.getOutputStream());
                         FileInputStream fis = new FileInputStream(file)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        while ((bytesRead = fis.read(buffer)) != -1) {
                            outData.write(buffer, 0, bytesRead);
                        }
                        outData.flush();
                        System.out.println("[Dstore] File " + filename + " sent successfully");
                    }
                } else {
                    System.out.println("[Dstore] File not found: " + filename + ". Closing socket.");
                }
            }

        } 

    } catch (Exception e) {
        System.out.println("[Dstore] Error handling client: " + e.getMessage());
    }
}



/**
 * prepears the given folder by creating it if it does not exsist and also
 * delete files inside the folder every time the dstore are connected again.
 * @param fileFolder the directory to prepare
 * @return true if the folder is ready for use false if not 
 */
private static boolean prepareFolder(String fileFolder) {
    File folder = new File(fileFolder);

    if (!folder.exists()) {
        if (!folder.mkdir()) {
            System.out.println("[Dstore]Could not create folder: " + fileFolder);
            return false;
        } else {
            System.out.println("[Dstore] Created folder: " + fileFolder);
        }
    }

    File[] files = folder.listFiles();
    if (files != null) {
        for (File f : files) {
            if (f.isFile()) f.delete();
        }
    }

    System.out.println("[Dstore] Folder refreshed: " + fileFolder);
    return true;
}


/**
 * handles commands from recieved by the controller for the remove operation
 * @param socket socket connected to the Controller
 * @param fileFolder the file locations in the directory
 */
private static void handleControllerCommands(Socket socket, String fileFolder) {
    try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
        String msg;
        while ((msg = in.readLine()) != null) {
            System.out.println("[Dstore]  From Controller: " + msg);

            if (msg.startsWith(Protocol.REMOVE_TOKEN)) {
                String[] parts = msg.split(" ");
                if (parts.length == 2) {
                    String filename = parts[1];
                    File file = new File(fileFolder, filename);

                    if (file.exists()) {
                        if (file.delete()) {
                            System.out.println("[Dstore] File deleted: " + filename);
                            controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                            System.out.println("[Dstore] Sent REMOVE_ACK for: " + filename);
                        } else {
                            System.out.println("[Dstore] Failed to delete file: " + filename);
                        }
                    } else {
                        controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                        System.out.println("[Dstore] File not found, sent ERROR_FILE_DOES_NOT_EXIST for: " + filename);
                    }
                }
            } else {
                System.out.println("[Dstore] Unknown message from controller: " + msg);
            }
        }
    } catch (IOException e) {
        System.out.println("[Dstore] Controller connection lost: " + e.getMessage());
    }
}


}