package ca.concordia.server;
import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;

public class FileServer {

    private FileSystemManager fsManager;
    private int port;
    public FileServer(int port, String fileSystemName, int totalSize){
        // Initialize the FileSystemManager
        FileSystemManager fsManager = new FileSystemManager(fileSystemName,
                10*128 );
        this.fsManager = fsManager;
        this.port = port;
    }

    public void start(){
        try (ServerSocket serverSocket = new ServerSocket(12345)) {
            System.out.println("Server started. Listening on port 12345...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Handling client: " + clientSocket);
                try (
                        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
                ) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Received from client: " + line);
                        String[] parts = line.split(" ");
                        String command = parts[0].toUpperCase();

                        switch (command) {
                            case "CREATE":
                                if (parts.length < 2) {
                                    writer.println("ERROR: usage CREATE <filename>"); break;
                                }
                                try {
                                    fsManager.createFile(parts[1]);
                                    writer.println("SUCCESS: created " + parts[1]);
                                }
                                catch (Exception e) {
                                    writer.println(e.getMessage());
                                }
                                writer.flush();
                                break;
                            //TODO: Implement other commands READ, WRITE, DELETE, LIST
                            case "WRITE":
                                if (parts.length < 3) { writer.println("ERROR: usage WRITE <filename> <content>"); break; }
                                try { fsManager.writeFile(parts[1], parts[2].getBytes());
                                    writer.println("SUCCESS: wrote to " + parts[1]);
                                }
                                catch (Exception e) {
                                    writer.println(e.getMessage());
                                }
                                writer.flush();
                                break;
                            case "READ":

                                if (parts.length < 2) { writer.println("ERROR: usage READ <filename>"); break; }
                                try {
                                    writer.println(new String(fsManager.readFile(parts[1]), StandardCharsets.US_ASCII));
                                }
                                catch (Exception e) { writer.println(e.getMessage()); }
                                break;

                            case "DELETE":
                                if (parts.length < 2) { writer.println("ERROR: usage DELETE <filename>"); break; }
                                try {
                                    fsManager.deleteFile(parts[1]);
                                    writer.println("SUCCESS: deleted " + parts[1]);
                                }
                                catch (Exception e) {
                                    writer.println(e.getMessage());
                                }
                                writer.flush();
                                break;

                            case "QUIT":
                                writer.println("SUCCESS: Disconnecting.");
                                return;
                            default:
                                writer.println("ERROR: Unknown command.");
                                break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        clientSocket.close();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }

}
