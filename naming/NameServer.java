package naming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

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

    public void registrationHandler(HttpExchange exchange) throws IOException{
        RegisterRequest data = new RegisterRequest();
        if (exchange.getRequestMethod() != "POST") {
            System.out.println("Method not allowed");
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }

         try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            data = gson.fromJson(requestBody, RegisterRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
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

            if (!(data.files.size() == 1 && data.files.get(0).equals("/"))){
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
            sendJsonResponse(exchange, 200, response);
        } else{
            ErrorRegistrationResponse response = new ErrorRegistrationResponse("IllegalStateException", "This storage server is already registered.");
            sendJsonResponse(exchange, 500, response);
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

    public void isDirectoryHandler(HttpExchange exchange) throws IOException{
        if (exchange.getRequestMethod() != "POST"){
            System.out.println("Method not allowed");
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }
        boolean isDir = true;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            PathRequest pathReq = gson.fromJson(requestBody, PathRequest.class);

            
            // Validate path
            if (pathReq.path == null || pathReq.path.isEmpty()) {
                sendErrorResponse(exchange, "IllegalArgumentException", "Path cannot be empty");
                return;
            }

            // Process the request
            pathReq.path = Util.sanitizePath(pathReq.path);

           
            if (pathReq.path == "") {
                sendErrorResponse(exchange,"IllegalArgumentException", 
                        "An error occurred while processing the request");
                return;
            } else {
                if (pathReq.path == "/") {
                    isDir = true;
                } else {
                    TreeNode node = this.fileSystem.findNode(pathReq.path);
                    if (node == null) {
                        isDir = false;
                        sendErrorResponse(exchange,"FileNotFoundException", 
                                "An error occurred while processing the request");
                        return;
                    } else {
                        isDir = node.isDir();
                    }
                }
            }
        } catch (Exception e) {
            // Handle unexpected exceptions
            e.printStackTrace();
            sendErrorResponse(exchange, "InternalServerError", "An error occurred while processing the request");
        }

        BooleanReturn response = new BooleanReturn(isDir);
        sendJsonResponse(exchange, 200, response);
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

    // /list endpoint for lsiting directory contents
    public void listHandler(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            PathRequest pathReq = gson.fromJson(requestBody, PathRequest.class);
            
            // Validate path
            if (pathReq.path == null || pathReq.path.isEmpty()) {
                sendErrorResponse(exchange, "IllegalArgumentException", "Path cannot be empty");
                return;
            }
            
            // Process the request
            pathReq.path = Util.sanitizePath(pathReq.path);
            List<String> files = findFiles(pathReq.path);
            
            // Create and send response
            FilesReturn response = new FilesReturn(files);
            sendJsonResponse(exchange, 200, response);
            
        } catch (ExceptionReturn e) {
            // Handle known exceptions from findFiles
            sendErrorResponse(exchange, e.exception_type, e.exception_info);
        } catch (Exception e) {
            // Handle unexpected exceptions
            e.printStackTrace();
            sendErrorResponse(exchange, "InternalServerError", "An error occurred while processing the request");
        }
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
    public void createDirectoryHandler(HttpExchange exchange) throws IOException{
        
        PathRequest pathReq = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }


        try{
            createDirectoryHelper(pathReq.path);
        } catch (Exception e) {
            sendErrorResponse(exchange, e.getClass().getSimpleName(), e.getMessage());
            return;
        }
    }

    // /create_file endpoint for creating a new file
    public void createFileHandler(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        PathRequest pathReq = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathReq = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(this.registeredNodes.isEmpty()){
            sendErrorResponse(exchange, "IllegalStateException", "No storage servers are registered with the naming server.");
            return;
        }

        try{
            createFileHelper(pathReq.path);
        } catch (Exception e) {
            sendErrorResponse(exchange, e.getClass().getSimpleName(), e.getMessage());
            return;
        }

        BooleanReturn response = new BooleanReturn(true);
        sendJsonResponse(exchange, 200, response);

    }

    public void getStorage(HttpExchange exchange) throws IOException{
        PathRequest pathReq = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
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
            sendErrorResponse(exchange, error, error);
            return;
        }

        ServerInfo serverInfo = new ServerInfo("127.0.0.1", node.sourcePorts.get(0));
        sendJsonResponse(exchange, 200, serverInfo);

    }

    // /lock endpoint for locking a file
    public void lockHandler(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        LockRequest lockRequest = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            lockRequest = gson.fromJson(requestBody, LockRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }

        if(lockRequest.path.isEmpty()){
            sendErrorResponse(exchange, "IllegalArgumentException", "Path cannot be empty");
            return;
        }

        lockRequest.path = Util.sanitizePath(lockRequest.path);
        
        try{
            this.fileSystem.lock(lockRequest.path, lockRequest.exclusive, this.registeredNodes, this.clientPorts, this.portMap);
        } catch (ExceptionReturn e) {
            sendErrorResponse(exchange, e.exception_type, e.exception_info);
        }
    }

    // /unlock endpoint for unlocking a file
    public void unlockHandler(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            LockRequest lockRequest = gson.fromJson(requestBody, LockRequest.class);

            if (lockRequest.path == null || lockRequest.path.isEmpty()) {
                sendErrorResponse(exchange, "IllegalArgumentException", "Path cannot be empty");
                return;
            }

            lockRequest.path = Util.sanitizePath(lockRequest.path);
            this.fileSystem.unlock(lockRequest.path, lockRequest.exclusive);
            
            // Send success response
            sendJsonResponse(exchange, 200, new BooleanReturn(true));
            
        } catch (ExceptionReturn e) {
            sendErrorResponse(exchange, e.exception_type, e.exception_info);
        } catch (Exception e) {
            e.printStackTrace();
            sendErrorResponse(exchange, "InternalServerError", "An error occurred while processing the request");
        }
    }

    public void deleteHandler(HttpExchange exchange) throws IOException{
        if (!exchange.getRequestMethod().equals("POST")) {
            sendErrorResponse(exchange, "MethodNotAllowedException", "Method not allowed");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            LockRequest lockRequest = gson.fromJson(requestBody, LockRequest.class);

            if (lockRequest.path == null || lockRequest.path.isEmpty()) {
                sendErrorResponse(exchange, "IllegalArgumentException", "Path cannot be empty");
                return;
            }

            lockRequest.path = Util.sanitizePath(lockRequest.path);
            this.fileSystem.deleteFile(lockRequest.path, this.portMap, this.registeredNodes);

            // Send success response
            sendJsonResponse(exchange, 200, new BooleanReturn(true));

        } catch (ExceptionReturn e) {
            sendErrorResponse(exchange, e.exception_type, e.exception_info);
        } catch (Exception e) {
            e.printStackTrace();
            sendErrorResponse(exchange, "InternalServerError", "An error occurred while processing the request");
        }
    }
    
    // Helper method to send JSON responses
    private void sendJsonResponse(HttpExchange exchange, int statusCode, Object response) throws IOException {
        String responseBody = gson.toJson(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBody.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBody.getBytes());
        }
    }

    // Helper method to send error responses
    private void sendErrorResponse(HttpExchange exchange, String errorType, String errorMessage) throws IOException {
        ExceptionReturn errorResponse = new ExceptionReturn(errorType, errorMessage);
        byte[] responseBytes = gson.toJson(errorResponse).getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(400, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected one argument: port");
        }

        int port = Integer.parseInt(args[0]);
        NameServer nameServer = new NameServer();
        
        // Initialize the file system
        nameServer.fileSystem = new TreeNode("", true, -1);
        nameServer.registeredNodes = new ArrayList<>();
        nameServer.clientPorts = new ArrayList<>();
        nameServer.portMap = new HashMap<>();
        
        // Create HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        
        // Set up context handlers
        server.createContext("/is_directory", new HttpHandler(){
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.isDirectoryHandler(exchange);
            }
        });

        server.createContext("/register", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.registrationHandler(exchange);
            }
        });
        
        // Set up other endpoints
        server.createContext("/lock", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.lockHandler(exchange);
            }
        });
        
        server.createContext("/unlock", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.unlockHandler(exchange);
            }
        });
        
        server.createContext("/create_file", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.createFileHandler(exchange);
            }
        });
        
        server.createContext("/create_directory", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.createDirectoryHandler(exchange);
            }
        });
        
        server.createContext("/delete", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nameServer.deleteHandler(exchange);
            }
        });
        
        // Start the server
        server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
        server.start();
        System.out.println("NameServer started on port " + port);
    }


}
