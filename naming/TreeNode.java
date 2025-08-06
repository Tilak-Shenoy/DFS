package naming;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import common.CopyRequest;

import com.google.gson.Gson;
import common.ExceptionReturn;
import common.LockRequest;
import common.PathRequest;


public class TreeNode {
    private static final Gson gson = new Gson();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantLock queueLock = new ReentrantLock();
    private final String key;
    private boolean isDir;
    private List<Integer> sourcePorts;
    private final AtomicInteger replicationReadCount = new AtomicInteger(0);
    private final Map<String, TreeNode> children = new ConcurrentHashMap<>();
    private final AtomicInteger readCount = new AtomicInteger(0);
    private final Queue<AccessRequest> accessQueue = new LinkedList<>();
    private boolean writeAccess = false;

    public TreeNode(String key, boolean isDir, List<Integer> source) {
        this.key = key;
        this.isDir = isDir;
        this.sourcePorts = new ArrayList<>();
        this.sourcePorts.addAll(source);
    }

    public TreeNode findNode(String path) {
        if (path == "/") {
            return this;
        }

        String[] parts = path.split("/");
        TreeNode current = this;

        for (String part : parts) {
            if (part == "") {
                continue;
            }
            TreeNode child = current.children.get(part);
            if (child == null) {
                return null;
            }
            current = child;
        }

        return current;
    }

    public void lock(String path, Boolean exclusive, List<Integer> storageNodes, List<Integer> client_ports,
            Map<Integer, Integer> port_map) throws ExceptionReturn {
        TreeNode node = this.findNode(path);
        if (node == null) {
            throw new ExceptionReturn("FILE_NOT_FOUND_EXCEPTION", path + " does not exist");
        }
        String[] parts = path.split("/");
        TreeNode current = this;

        if (path == "/") {
            current.queueLock.lock();
            AccessRequest request = new AccessRequest(exclusive);
            current.accessQueue.add(request);
            current.queueLock.unlock();

            processQueue(current);
            request.done.complete(true);
            return;
        }

        String lastElement = parts[parts.length - 1];

        for(String part:parts){
            current = current.children.get(part);
            
            //Start from the last element
            if (current.key == lastElement){
                //Release queue lock to append into the queue
                current.queueLock.lock();
                AccessRequest request = new AccessRequest(exclusive);
                current.accessQueue.add(request);
                current.queueLock.unlock();

                processQueue(current);
                request.done.complete(true);

                if(!exclusive) {
                    current.replicationReadCount.incrementAndGet();
                    int readCount = current.replicationReadCount.get();
                    // Replicate in other servers if the file is accessed frequently
                    if(readCount >= 20){
                        current.replicationReadCount.set(0);
                        handleReplication(current, storageNodes, path, client_ports, port_map);
                    }
                } else {
                    current.replicationReadCount.set(0);
                    handleReplicaDeletion(current, path, port_map);
                }
            } else {
                current.queueLock.lock();
                AccessRequest request = new AccessRequest(false);
                current.accessQueue.add(request);
                current.queueLock.unlock();

                processQueue(current);
                request.done.complete(true);
            }
        }
        return;
    }

    private void processQueue(TreeNode current) {
        current.queueLock.lock();
        try {
            while(!current.accessQueue.isEmpty()) {
                AccessRequest request = current.accessQueue.poll();
                //if request is exclusive and no other process has access
                if(request.isExclusive() && current.readCount.get() == 0 && !current.writeAccess) {
                    current.rwLock.writeLock().lock();
                    try {
                        current.writeAccess = true;
                        request.done.complete(true);
                    } finally {
                        current.rwLock.writeLock().unlock();
                    }
                }
                //if request is shared and no other process has exclusive access
                else if(!request.isExclusive() && !current.writeAccess) {
                    current.rwLock.readLock().lock();
                    try {
                        current.readCount.incrementAndGet();
                        request.done.complete(true);
                    } finally {
                        current.rwLock.readLock().unlock();
                    }
                } else {
                    // Requeue the request if we can't process it now
                    current.accessQueue.add(request);
                    break;
                }
            }
        } finally {
            current.queueLock.unlock();
        }
    }

    private void handleReplication(TreeNode current, List<Integer> storageNodes, String path, List<Integer> client_ports, Map<Integer, Integer> port_map){
        Integer sourceNode = current.sourcePorts.get(0);
        Map<Integer, Boolean> sourceMap = new HashMap<>();
        for(Integer node: current.sourcePorts){
            sourceMap.put(node, true);
        }

        for(Integer node: client_ports){
            if(!sourceMap.containsKey(node)){
                Integer port = port_map.get(node);
                callStorageServerToReplicate(String.format("http://127.0.0.1:%d/storage_copy", port), path, sourceNode);
                current.sourcePorts.add(node);
                break;
            }
        }

    }

    private void callStorageServerToReplicate(String format, String path, Integer sourceNode) {
        CopyRequest requestObj = new CopyRequest(path, "127.0.0.1", sourceNode);
        String requestBody = gson.toJson(requestObj);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(format))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }

    private void handleReplicaDeletion(TreeNode current, String path, Map<Integer, Integer> port_map) {
        List<Integer> node_list = current.sourcePorts;
        if (node_list.size() == 1) {
            return;
        }

        current.sourcePorts = List.of(node_list.get(0));
        System.out.println("Deleting copies on:");
        for(int i = 1; i < node_list.size(); i++){
            System.out.println(node_list.get(i));
            callStorageServer(String.format("http://127.0.0.1:%d/storage_delete", port_map.get(node_list.get(i))), path);
            System.out.println("Deleted");
        }
        
    }

    private void callStorageServer(String url, String path){
        PathRequest requestPath = new PathRequest(path);
        String requestBody = gson.toJson(requestPath);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
