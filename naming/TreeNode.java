package naming;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.Gson;
import common.ExceptionReturn;
import util.Util;


public class TreeNode {
    public static final Gson gson = new Gson();
    public final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    public final ReentrantLock queueLock = new ReentrantLock();
    public final String key;
    public boolean isDir;
    public List<Integer> sourcePorts;
    public final AtomicInteger replicationReadCount = new AtomicInteger(0);
    public final Map<String, TreeNode> children = new ConcurrentHashMap<>();
    public final AtomicInteger readCount = new AtomicInteger(0);
    public final Queue<AccessRequest> accessQueue = new LinkedList<>();
    public boolean writeAccess = false;

    public boolean isDir(){
        return this.isDir;
    }

    public TreeNode(String key, boolean isDir, int source) {
        this.key = key;
        this.isDir = isDir;
        this.sourcePorts = new ArrayList<>();
        this.sourcePorts.add(source);
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
                        Util.handleReplication(current, storageNodes, path, client_ports, port_map);
                    }
                } else {
                    current.replicationReadCount.set(0);
                    Util.handleReplicaDeletion(current, path, port_map);
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
                        //TS: Remove this?
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


    public void unlock(String path, Boolean exclusive) throws ExceptionReturn{
        TreeNode node = this.findNode(path);
        if(node == null){
            throw new ExceptionReturn("IllegalArgumentException", "IllegalArgumentException");
        }

        TreeNode current = this;

        if(path == "/"){
            if(exclusive){
                // Release the exclusive lock
                current.writeAccess = false;
                current.rwLock.writeLock().unlock();
                processQueue(current);
            } else {
                // Release the shared lock
                current.readCount.decrementAndGet();
                current.rwLock.readLock().unlock();
                processQueue(current);
            }
            return;
        }

        String[] parts = path.split("/");
        String lastElement = parts[parts.length - 1];
        for(String part: parts){
            current = current.children.get(part);
            if(current.key == lastElement && exclusive){
                // Release the exclusive lock
                current.writeAccess = false;
                current.rwLock.writeLock().unlock();
                processQueue(current);
            } else {
                // Release the shared lock
                current.readCount.decrementAndGet();
                current.rwLock.readLock().unlock();
                processQueue(current);
            }
        }        
    }
    
    // Adds a file to the tree based on the path
    public void addFile(String path, boolean isDir, int source){
        String[] parts= path.split("/");
        TreeNode current = this;

        this.rwLock.writeLock().lock();
        this.writeAccess = true;
        
        for(String part: parts){
            if(part == ""){
                continue;
            }
            
            boolean isCurrentDir = part.length() < parts.length - 1;
            
            if(current.children.get(part) == null){
                current.children.put(part, new TreeNode(part, isCurrentDir, source));
            }
            current = current.children.get(part);
        }
        
        current.isDir = isDir;
        this.writeAccess = false;
        this.rwLock.writeLock().unlock();
    }
    public void printTree(int level){
        throw new UnsupportedOperationException();
    }

    public void deleteFile(String path, Map<Integer, Integer> portMap, List<Integer> nodes) throws ExceptionReturn{
        TreeNode node = this.findNode(path);
        if(node == null){
            throw new ExceptionReturn("FileNotFoundException", "The file/directory or parent directory does not exist.");
        }

        this.rwLock.writeLock().lock();
        this.writeAccess = true;
        
        try{
            //TS: should it be portMap[src]
            for (int source: nodes){
                Util.callStorageServer(String.format("http://127.0.0.1:%d/storage_delete", portMap.get(source)), path);
            }
        } finally {
            this.writeAccess = false;
            this.rwLock.writeLock().unlock();
        }
    }
    
}
