package naming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;

import com.google.gson.Gson;

import common.BooleanReturn;
import common.ErrorRegistrationResponse;
import common.ExceptionReturn;
import common.FilesReturn;
import common.LockRequest;
import common.PathRequest;
import common.RegisterRequest;
import common.ServerInfo;
import common.SuccessfulRegistrationResponse;
import util.Util;

public class NameServer {
    static Gson gson = new Gson();
    public TreeNode fileSystem;
    public int servicePort;
    public String registrationPort;
    public List<Integer> registeredNodes;
    public List<Integer> clientPorts;
    public Map<Integer, Integer> portMap;
    public Map<String, List<Integer>> filesDict;
    public final ReentrantLock mu = new ReentrantLock();

    public NameServer(){}
    private String getParentDir(String path){
        return path.substring(0, path.lastIndexOf('/'));
    }

    private boolean checkIfNodeRegistered(int newNode){
        this.mu.lock();
        for (Integer node: this.registeredNodes){
            if (node == newNode){
                this.mu.unlock();
                return true;
            }
        }
        this.mu.unlock();
        return false;
    }

    public void registrationHandler(HttpServletResponse w, HttpServletRequest r) throws IOException{
        RegisterRequest data = new RegisterRequest();
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            data = gson.fromJson(requestBody, RegisterRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        int node = data.command_port;
        int clientPort = data.client_port;
        boolean isRegistered = this.checkIfNodeRegistered(node);
        if (!isRegistered){
            this.registeredNodes.add(node);
            this.portMap.put(clientPort, node);
            this.clientPorts.add(clientPort);

            List<String> deleted = new ArrayList<String>();

            this.fileSystem.printTree(1);

            if (!(data.files.length == 1 && data.files[0].equals("/"))){
                this.mu.lock();
                for (String filename: data.files){
                    TreeNode newNode = this.fileSystem.findNode(filename);
                    if (newNode !=null){
                        deleted.add(filename);
                    }
                    else{
                        this.fileSystem.addFile(filename, false, clientPort);
                        updateDict(filename, node, this.filesDict);
                    }
                    
                }
                this.mu.unlock();
            }

            this.fileSystem.printTree(1);

            SuccessfulRegistrationResponse response = new SuccessfulRegistrationResponse(deleted.toArray(new String[deleted.size()]));
            w.setContentType("application/json");
            w.setStatus(HttpServletResponse.SC_OK);
            w.getWriter().write(gson.toJson(response));
        } else{
            ErrorRegistrationResponse response = new ErrorRegistrationResponse("IllegalStateException", "This storage server is already registered.");
            w.setContentType("application/json");
            w.setStatus(HttpServletResponse.SC_CONFLICT);
            w.getWriter().write(gson.toJson(response));
        }
    }


    private void updateDict(String filename, int node, Map<String, List<Integer>> dict){
        String[] parts = filename.split("/");
        for(String part: parts){
            if (part == ""){
                continue;
            }
            if (!dict.containsKey(part)){
                dict.put(part, new ArrayList<Integer>());
            }
            dict.get(part).add(node);
        }
    }

    public void isDirectoryHandler(HttpServletResponse w, HttpServletRequest r) throws IOException{
        if (r.getMethod() != "POST"){
            System.out.println("Method not allowed");
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        PathRequest pathReq = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.fileSystem.printTree(1);

        if(pathReq.path != null && pathReq.path != "" ){
            pathReq.path = Util.sanitizePath(pathReq.path);
        }

        String exceptionTypeString="";
        boolean isDir = true;
        if(pathReq.path ==""){
            exceptionTypeString="IllegalArgumentException";
        } else{
            if(pathReq.path == "/"){
                isDir = true;
            } else{
                TreeNode node = this.fileSystem.findNode(pathReq.path);
                if(node == null){
                    isDir = false;
                    exceptionTypeString="FileNotFoundException";
                } else{
                    isDir = node.isDir();
                }
            }
        }

        if(exceptionTypeString != ""){
            exceptionHanler(w, exceptionTypeString, "the file/directory or parent directory does not exist.");
            return;
        }

        BooleanReturn response = new BooleanReturn(isDir);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }

    private List<String> findFiles(String path) throws ExceptionReturn{
        TreeNode node = this.fileSystem.findNode(path);
        if(node == null){
            throw new ExceptionReturn("FileNotFoundException", "the file/directory or parent directory does not exist.");
        }
        if(!node.isDir){
            throw new ExceptionReturn("IllegalArgumentException", "the file/directory or parent directory does not exist.");
        }
        return node.children.keySet().stream().collect(Collectors.toList());
    }
    

    private void exceptionHanler(HttpServletResponse w, String exceptionTypeString, String exceptionInfo) throws IOException{
        ExceptionReturn response = new ExceptionReturn(exceptionTypeString, exceptionInfo);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_NOT_FOUND);
        w.getWriter().write(gson.toJson(response));
    }

    // /list endpoint for lsiting directory contents
    public void listHandler(HttpServletResponse w, HttpServletRequest r) throws IOException{
        if (r.getMethod() != "POST") {
            System.out.println("Method not allowed");
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        PathRequest pathReq = new PathRequest("");
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> files = new ArrayList<>();
        String error = "";

        if(pathReq.path != ""){
            try {
                files = this.findFiles(pathReq.path);
            } catch (ExceptionReturn e) {
                error = e.exception_type;
            }
        } else{
            error = "IllegalArgumentException";
        }


        if(!error.isEmpty()){
            exceptionHanler(w, error, "the file/directory or parent directory does not exist.");
            return;
        }

        FilesReturn response = new FilesReturn(files);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }


    private  void createDirectoryHelper(String path) throws FileNotFoundException{
        if(path.isEmpty()){
            throw new IllegalArgumentException();
        }

        TreeNode node = this.fileSystem.findNode(path);
        if(node == null){
            throw new FileNotFoundException();
        }

        if(!node.isDir){
            throw new FileNotFoundException();
        }

        this.fileSystem.addFile(path, true, 0);
    }

    private void createFileHelper(String path) throws ExceptionReturn, FileNotFoundException{
        if(path.isEmpty()){
            throw new IllegalArgumentException();
        }

        TreeNode node = this.fileSystem.findNode(path);
        if(node == null){
            throw new FileNotFoundException();
        }

        String parent = getParentDir(path);
        
        TreeNode parentNode = this.fileSystem.findNode(parent);
        if(parentNode == null){
            throw new FileNotFoundException();
        }

        if(!parentNode.isDir){
            throw new FileNotFoundException();
        }

        this.fileSystem.addFile(path, false, 0);
        // TS: By default, the data is stored in the first storage server and replicated as the file gets read over the threshold?
        Util.callStorageServer(String.format("http://127.0.0.1:%d/storage_create", this.registeredNodes.get(0)), path);

    }

    // /create_directory endpoint for creating a directory
    public void createDirectoryHandler(HttpServletResponse w, HttpServletRequest r) throws IOException{
        
        PathRequest pathReq = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }


        try{
            createDirectoryHelper(pathReq.path);
        } catch (Exception e) {
            exceptionHanler(w, e.getClass().toString(), e.getMessage());
            return;
        }
    }

    // /create_file endpoint for creating a new file
    public void createFileHandler(HttpServletResponse w, HttpServletRequest r) throws IOException{
        if (r.getMethod() != "POST") {
            System.out.println("Method not allowed");
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        PathRequest pathReq = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(this.registeredNodes.isEmpty()){
            exceptionHanler(w, "IllegalStateException", "No storage servers  are registered with the naming server.");
            return;
        }

        try{
            createFileHelper(pathReq.path);
        } catch (Exception e) {
            exceptionHanler(w, e.getClass().toString(), e.getMessage());
            return;
        }

        BooleanReturn response = new BooleanReturn(true);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));

    }

    public void getStorage(HttpServletResponse w, HttpServletRequest r) throws IOException{
        PathRequest pathReq = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }

        String error = "";
        TreeNode node = null;

        if (pathReq.path.isEmpty()){
            error="IllegalArgumentException";
        } else{
            pathReq.path = Util.sanitizePath(pathReq.path);
            node = this.fileSystem.findNode(pathReq.path);

            if(node == null || node.isDir){
                error = "FileNotFoundException";
            }
        }

        if(error.isEmpty()){
            exceptionHanler(w, error, error);
            return;
        }

        ServerInfo serverInfo = new ServerInfo("127.0.0.1", node.sourcePorts.get(0));
        w.setHeader("Content-Type","application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(serverInfo));

    }

    // /lock endpoint for locking a file
    public void lockHandler(HttpServletResponse w, HttpServletRequest r ) throws IOException{
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        LockRequest lockRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            lockRequest = gson.fromJson(requestBody, LockRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }

        if(lockRequest.path.isEmpty()){
            exceptionHanler(w, "IllegalArgumentException", "IllegalArgumentException");
            return;
        }

        lockRequest.path = Util.sanitizePath(lockRequest.path);
        
        try{
            this.fileSystem.lock(lockRequest.path, lockRequest.exclusive, this.registeredNodes, this.clientPorts, this.portMap);
        } catch (ExceptionReturn e) {
            exceptionHanler(w, e.exception_type, e.exception_info);
        }
    }

    // /unlock endpoint for unlocking a file
    public void unlockHandler(HttpServletResponse w, HttpServletRequest r ) throws IOException{
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        LockRequest lockRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            lockRequest = gson.fromJson(requestBody, LockRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }

        if(lockRequest.path.isEmpty()){
            exceptionHanler(w, "IllegalArgumentException", "IllegalArgumentException");
            return;
        }

        lockRequest.path = Util.sanitizePath(lockRequest.path);
        
        try{
            this.fileSystem.unlock(lockRequest.path, lockRequest.exclusive);
        } catch (ExceptionReturn e) {
            exceptionHanler(w, e.exception_type, e.exception_info);
        }
    }

    public void deleteHandler(HttpServletResponse w, HttpServletRequest r) throws IOException{
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        LockRequest lockRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            lockRequest = gson.fromJson(requestBody, LockRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }

        if(lockRequest.path.isEmpty()){
            exceptionHanler(w, "IllegalArgumentException", "IllegalArgumentException");
            return;
        }

        lockRequest.path = Util.sanitizePath(lockRequest.path);
        
        try{
            this.fileSystem.deleteFile(lockRequest.path, this.portMap, this.registeredNodes);
        } catch (ExceptionReturn e) {
            exceptionHanler(w, e.exception_type, e.exception_info);
        }

        BooleanReturn response = new BooleanReturn(true);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }


    public static void main(String[] args) {
        //TODO: Finish this
    }


}
