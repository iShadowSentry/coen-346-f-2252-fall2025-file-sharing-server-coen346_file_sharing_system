package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class FileServer {

    private final FileSystemManager fsManager;
    private final int port;

    public FileServer(int port, String fileSystemName, int totalSize){
        this.fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.port = port;
    }

    // ------------ Handle ONE command per client ---------------
    private void handleClient(Socket clientSocket) {
        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            // TEST CLIENT sends exactly ONE line then closes
            String line = reader.readLine();
            if (line == null) return;

            System.out.println("Received from client: " + line);

            String[] parts = line.split(" ", 3);
            String command = parts[0].toUpperCase();

            try {
                // 🔒 serialize ALL filesystem access here
                synchronized (fsManager) {
                    switch (command) {

                        case "CREATE": {
                            if (parts.length < 2) {
                                writer.println("ERROR: usage CREATE <filename>");
                                break;
                            }
                            fsManager.createFile(parts[1]);
                            writer.println("SUCCESS");
                            break;
                        }

                        case "WRITE": {
                            if (parts.length < 3) {
                                writer.println("ERROR: usage WRITE <filename> <content>");
                                break;
                            }
                            fsManager.writeFile(parts[1], parts[2].getBytes());
                            writer.println("SUCCESS");
                            break;
                        }

                        case "READ": {
                            if (parts.length < 2) {
                                writer.println("ERROR: usage READ <filename>");
                                break;
                            }
                            byte[] data = fsManager.readFile(parts[1]);
                            String out = new String(data);
                            out = out.replace("\n", "");
                            writer.println(out.isEmpty() ? "EMPTY" : out);
                            break;
                        }

                        case "DELETE": {
                            if (parts.length < 2) {
                                writer.println("ERROR: usage DELETE <filename>");
                                break;
                            }
                            fsManager.deleteFile(parts[1]);
                            writer.println("SUCCESS");
                            break;
                        }

                        case "LIST": {
                            String[] files = fsManager.listFiles();
                            if (files.length == 0) {
                                writer.println("NO_FILES");
                            } else {
                                writer.println(String.join(",", files));
                            }
                            break;
                        }

                        default:
                            writer.println("ERROR: Unknown command");
                    }
                } // end synchronized

            } catch (Exception e) {
                writer.println("ERROR: " + e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { clientSocket.close(); } catch (Exception ignore) {}
        }
    }

    // -------- Main accept loop: one thread per connection --------
    public void start(){
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");

            while (true) {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClient(client)).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
