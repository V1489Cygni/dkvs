package dkvs.server;

import dkvs.server.messages.Message;
import dkvs.server.util.Configuration;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ConnectionManager extends Thread {
    private final NodeThread thread;
    private final Map<SocketAddress, Connection> connections = new HashMap<>();
    private final ServerSocket serverSocket;
    private volatile boolean finished;

    public ConnectionManager(Configuration configuration, NodeThread thread) throws IOException {
        this.thread = thread;
        serverSocket = new ServerSocket(configuration.ports[configuration.number]);
    }

    @Override
    public void run() {
        while (!finished) {
            try {
                Socket socket = serverSocket.accept();
                SocketAddress address = socket.getRemoteSocketAddress();
                synchronized (connections) {
                    if (connections.containsKey(address)) {
                        connections.get(address).close();
                    }
                    Connection connection = new Connection(socket, thread);
                    connections.put(address, connection);
                    connection.start();
                }
            } catch (IOException e) {
            }
        }
    }

    public void send(Message message) {
        synchronized (connections) {
            SocketAddress address = message.address;
            if (!connections.containsKey(address) || connections.get(address).isClosed()) {
                Socket socket = new Socket();
                try {
                    socket.connect(address);
                    Connection connection = new Connection(socket, thread);
                    connections.put(address, connection);
                    connection.start();
                } catch (IOException e) {
                }
            }
            if (connections.get(address) != null) {
                connections.get(address).send(message);
            }
        }
    }

    public void close() {
        finished = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            synchronized (System.err) {
                System.err.println("Exception while closing server socket: " + e.getMessage());
            }
        }
        synchronized (connections) {
            connections.values().forEach(dkvs.server.Connection::close);
        }
    }
}
