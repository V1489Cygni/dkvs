package dkvs.server;

import dkvs.server.messages.Message;
import dkvs.server.messages.csp.ClientServerRequest;
import dkvs.server.messages.csp.ClientServerResponse;
import dkvs.server.messages.nnp.AppendEntriesRPC;
import dkvs.server.messages.nnp.AppendEntriesResult;
import dkvs.server.messages.nnp.RequestVoteRPC;
import dkvs.server.messages.nnp.RequestVoteResult;
import dkvs.server.util.Configuration;
import dkvs.server.util.Entry;
import dkvs.server.util.Operation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class NodeThread extends Thread {
    private final Configuration configuration;
    private final ConnectionManager manager;
    private final Queue<Message> queue = new ArrayDeque<>();
    private final Map<Integer, ClientServerRequest> requests = new HashMap<>();
    private final State state;
    private final int[] nextIndex, matchIndex;
    private long lastMessageTime;
    private int voteCount;
    private volatile boolean isStopped;
    private int leader = -1;

    public NodeThread(Configuration configuration) throws IOException {
        this.configuration = configuration;
        manager = new ConnectionManager(configuration, this);
        state = new State(configuration.number);
        nextIndex = new int[configuration.totalNumber];
        for (int i = 0; i < nextIndex.length; i++) {
            nextIndex[i] = state.log.size();
        }
        matchIndex = new int[configuration.totalNumber];
    }

    public void add(Message message) {
        synchronized (queue) {
            queue.add(message);
            queue.notify();
        }
    }

    private Message getMessage() throws InterruptedException {
        int timeout = getTimeout();
        while (true) {
            synchronized (queue) {
                if (!queue.isEmpty()) {
                    return queue.poll();
                } else if (System.currentTimeMillis() - lastMessageTime > timeout) {
                    return null;
                } else {
                    new Thread(() -> {
                        long t = System.currentTimeMillis();
                        while (System.currentTimeMillis() - t < timeout) {
                            try {
                                Thread.sleep(timeout - (System.currentTimeMillis() - t));
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                        synchronized (queue) {
                            queue.notify();
                        }
                    }).start();
                    queue.wait();
                }
            }
        }
    }

    @Override
    public void run() {
        manager.start();
        lastMessageTime = System.currentTimeMillis();
        while (!isStopped) {
            try {
                Message message = getMessage();
                if (message == null) {
                    processTimeout();
                } else {
                    processMessage(message);
                }
            } catch (InterruptedException | IOException e) {
                close();
                return;
            }
        }
    }

    private void processMessage(Message message) throws IOException {
        System.out.println(getStatus() + " Message received (" + message.address + "): " + message.prettyPrint());
        if (message instanceof ClientServerRequest) {
            processClientServerRequest((ClientServerRequest) message);
        } else if (message instanceof ClientServerResponse) {
            processClientServerResponse((ClientServerResponse) message);
        } else if (message instanceof AppendEntriesRPC) {
            processAppendEntriesRPC((AppendEntriesRPC) message);
        } else if (message instanceof AppendEntriesResult) {
            processAppendEntriesResult((AppendEntriesResult) message);
        } else if (message instanceof RequestVoteRPC) {
            processRequestVoteRPC((RequestVoteRPC) message);
        } else if (message instanceof RequestVoteResult) {
            processRequestVoteResult((RequestVoteResult) message);
        } else {
            throw new AssertionError();
        }
    }

    private void processClientServerRequest(ClientServerRequest request) throws IOException {
        switch (request.operation) {
            case PING:
                manager.send(getStatus(), new ClientServerResponse(request.address, Operation.PING, true, null, request.redirections));
                return;
            case GET:
                manager.send(getStatus(), new ClientServerResponse(request.address, Operation.GET,
                        state.data.containsKey(request.key), state.data.get(request.key), request.redirections));
                return;
            case SET:
            case DELETE:
                if (state.state == 2) {
                    state.add(new Entry[]{new Entry(state.term, request.operation, request.key, request.value)});
                    requests.put(state.log.size() - 1, request);
                    checkCommit();
                    processTimeout();
                } else if (leader != -1) {
                    ClientServerRequest r = new ClientServerRequest(
                            new InetSocketAddress(configuration.hosts[leader], configuration.ports[leader]),
                            request.operation, request.key, request.value, request.redirections);
                    r.redirections.add((InetSocketAddress) request.address);
                    manager.send(getStatus(), r);
                } else {
                    manager.send(getStatus(), new ClientServerResponse(request.address, request.operation, false, "Unknown leader", request.redirections));
                }
        }
    }

    private void processClientServerResponse(ClientServerResponse response) {
        ClientServerResponse r = new ClientServerResponse(response.redirections.get(response.redirections.size() - 1),
                response.operation, response.success, response.result, response.redirections);
        r.redirections.remove(r.redirections.size() - 1);
        manager.send(getStatus(), r);
    }

    private void processAppendEntriesRPC(AppendEntriesRPC rpc) throws IOException {
        if (rpc.term >= state.term) {
            if (state.state != 0) {
                System.out.println("Converting to follower.");
            }
            state.setTerm(rpc.term);
            state.state = 0;
            lastMessageTime = System.currentTimeMillis();
            leader = rpc.leaderId;
        }
        if (state.term == rpc.term && (rpc.prevLogIndex == -1
                || state.log.size() > rpc.prevLogIndex && state.log.get(rpc.prevLogIndex).term == rpc.prevLogTerm)) {
            state.remove(rpc.prevLogIndex);
            state.add(rpc.log);
            if (rpc.leaderCommit > state.commitNext) {
                state.setCommitNext(Math.min(rpc.leaderCommit, state.log.size()));
            }
            manager.send(getStatus(), new AppendEntriesResult(rpc.address, state.term, true, state.log.size(), configuration.number));
        } else {
            manager.send(getStatus(), new AppendEntriesResult(rpc.address, state.term, false, state.log.size(), configuration.number));
        }
    }

    private void processAppendEntriesResult(AppendEntriesResult result) throws IOException {
        if (result.success) {
            nextIndex[result.id] = result.currentLength;
            matchIndex[result.id] = result.currentLength;
            checkCommit();
        } else {
            nextIndex[result.id]--;
        }
    }

    private void processRequestVoteRPC(RequestVoteRPC rpc) throws IOException {
        if (rpc.term < state.term) {
            manager.send(getStatus(), new RequestVoteResult(rpc.address, state.term, false));
        } else {
            if (rpc.term > state.term) {
                if (state.state != 0) {
                    System.out.println("Converting to follower.");
                }
                state.setTerm(rpc.term);
                state.setVotedFor(-1);
                state.state = 0;
            }
            int llt = state.log.size() == 0 ? -2 : state.log.get(state.log.size() - 1).term;
            boolean upd = rpc.lastLogTerm > llt || rpc.lastLogTerm == llt && rpc.lastLogIndex >= state.log.size() - 1;
            if (upd && (state.votedFor == -1 || state.votedFor == rpc.candidateId)) {
                state.setVotedFor(rpc.candidateId);
                manager.send(getStatus(), new RequestVoteResult(rpc.address, state.term, true));
            } else {
                manager.send(getStatus(), new RequestVoteResult(rpc.address, state.term, false));
            }
        }
    }

    private void processRequestVoteResult(RequestVoteResult result) throws IOException {
        if (state.state == 1) {
            if (result.term == state.term && result.voteGranted) {
                voteCount++;
                if (voteCount > configuration.totalNumber / 2) {
                    System.out.println("Converting to leader.");
                    state.state = 2;
                    for (int i = 0; i < nextIndex.length; i++) {
                        nextIndex[i] = state.log.size();
                        matchIndex[i] = 0;
                    }
                    processTimeout();
                }
            } else if (result.term > state.term) {
                System.out.println("Converting to follower.");
                state.setTerm(result.term);
                state.setVotedFor(-1);
                state.state = 0;
            }
        }
    }

    private void processTimeout() throws IOException {
        if (state.state == 2) {
            for (int i = 0; i < configuration.totalNumber; i++) {
                if (i != configuration.number) {
                    Entry[] entries = new Entry[state.log.size() - nextIndex[i]];
                    for (int j = 0; j < entries.length; j++) {
                        entries[j] = state.log.get(nextIndex[i] + j);
                    }
                    manager.send(getStatus(), new AppendEntriesRPC(
                            new InetSocketAddress(configuration.hosts[i], configuration.ports[i]),
                            state.term, configuration.number, nextIndex[i] - 1,
                            nextIndex[i] == 0 ? -1 : state.log.get(nextIndex[i] - 1).term,
                            state.commitNext, entries));
                }
            }
        } else {
            if (state.state != 1) {
                System.out.println("Converting to candidate.");
            }
            leader = -1;
            state.setTerm(state.term + 1);
            state.state = 1;
            state.setVotedFor(configuration.number);
            voteCount = 1;
            if (configuration.totalNumber == 1) {
                state.state = 2;
                nextIndex[0] = state.log.size();
                matchIndex[0] = 0;
                System.out.println("Converting to leader.");
            }
            for (int i = 0; i < configuration.totalNumber; i++) {
                if (i != configuration.number) {
                    manager.send(getStatus(), new RequestVoteRPC(
                            new InetSocketAddress(configuration.hosts[i], configuration.ports[i]),
                            state.term, configuration.number, state.log.size() - 1,
                            state.log.size() == 0 ? -1 : state.log.get(state.log.size() - 1).term));
                }
            }
        }
        lastMessageTime = System.currentTimeMillis();
    }

    private void checkCommit() throws IOException {
        for (int i = state.commitNext; i < state.log.size(); i++) {
            int num = 1;
            for (int j = 0; j < configuration.totalNumber; j++) {
                if (configuration.number != j && matchIndex[j] > i) {
                    num++;
                }
            }
            if (num > configuration.totalNumber / 2 && state.log.get(i).term == state.term) {
                state.setCommitNext(i + 1);
            }
        }
    }

    private int getTimeout() {
        return state.state == 2 ? configuration.timeout / 2 : configuration.timeout;
    }

    public void close() {
        isStopped = true;
        manager.close();
        try {
            state.close();
        } catch (IOException e) {
        }
        interrupt();
    }

    private String getStatus() {
        return "[" + (state.state == 0 ? "FOLLOWER" : (state.state == 1 ? "CANDIDATE" : "LEADER")) + ", term=" +
                state.term + "]";
    }

    private class State {
        public final int number;
        public final Map<String, String> data = new HashMap<>();
        public final List<Entry> log = new ArrayList<>();
        public final FileWriter writer;

        public int state;

        public int commitNext;

        public int term, votedFor;

        public State(int number) throws IOException {
            this.number = number;
            String logFile = "dkvs_" + number + ".log";
            writer = new FileWriter(logFile, true);
            readLog(logFile);
            readState();
        }

        public void setTerm(int term) throws IOException {
            this.term = term;
            writeState();
        }

        public void setVotedFor(int votedFor) throws IOException {
            this.votedFor = votedFor;
            writeState();
        }

        public void add(Entry[] entries) {
            Collections.addAll(log, entries);
        }

        public void remove(int last) {
            for (int i = last + 1; i < log.size(); i++) {
                requests.remove(i);
            }
            while (log.size() > last + 1) {
                log.remove(log.size() - 1);
            }
        }

        public void setCommitNext(int commitNext) throws IOException {
            for (int i = this.commitNext; i < commitNext; i++) {
                applyEntry(i);
                writer.write(log.get(i) + "\n");
            }
            writer.flush();
            this.commitNext = commitNext;
        }

        private void readLog(String fileName) {
            try {
                Scanner sc = new Scanner(new File(fileName));
                while (sc.hasNext()) {
                    log.add(Entry.parseEntry(sc));
                    applyEntry(log.size() - 1);
                }
                commitNext = log.size();
                sc.close();
            } catch (FileNotFoundException e) {
            }
        }

        private void readState() {
            try {
                Scanner sc = new Scanner(new File(getStateFileName()));
                term = sc.nextInt();
                votedFor = sc.nextInt();
                sc.close();
            } catch (FileNotFoundException e) {
                votedFor = -1;
            }
        }

        private void writeState() throws IOException {
            FileWriter writer = new FileWriter(getStateFileName());
            writer.write(term + " " + votedFor);
            writer.close();
        }

        private void applyEntry(int number) {
            Entry entry = log.get(number);
            if (entry.operation.equals(Operation.SET)) {
                data.put(entry.key, entry.value);
                if (requests.containsKey(number)) {
                    manager.send(getStatus(), new ClientServerResponse(requests.get(number).address, requests.get(number).operation,
                            true, null, requests.get(number).redirections));
                }
            } else if (entry.operation.equals(Operation.DELETE)) {
                boolean success = data.containsKey(entry.key);
                data.remove(entry.key);
                if (requests.containsKey(number)) {
                    manager.send(getStatus(), new ClientServerResponse(requests.get(number).address, requests.get(number).operation,
                            success, null, requests.get(number).redirections));
                }
            }
        }

        private String getStateFileName() {
            return ".state_" + number;
        }

        public void close() throws IOException {
            writer.close();
        }
    }
}
