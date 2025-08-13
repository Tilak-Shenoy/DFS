package storage;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.w3c.dom.html.HTMLAppletElement;

import com.google.gson.Gson;

import common.BooleanReturn;
import common.CopyRequest;
import common.DataReturn;
import common.ExceptionReturn;
import common.PathRequest;
import common.ReadRequest;
import common.RegisterRequest;
import common.SizeReturn;
import common.SuccessfulRegistrationResponse;
import common.WriteRequest;
import util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class StorageServer {

    public int naminServerPort;
    public int clientPort;
    public int commandPort;
    public String rootPath;
    public List<String> files;

    public Gson gson = new Gson();

    private String getParentDir(String path) {
        return path.substring(0, path.lastIndexOf('/'));
    }

    public void createFile(String path){
        String dirPath = getParentDir(path);
        

        File dir = new File(dirPath);
        if (!dir.mkdirs()) {
            throw new RuntimeException("failed to create directories: " + dirPath);
        }

        File file = new File(path);
        try {
            if (!file.createNewFile()) {
                throw new RuntimeException("failed to create file: " + path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean createFileMain(String path) {
        String error = "";
        
        if (path.isEmpty()) {
            System.out.println("empty arg error during create dir");
            error = "IllegalArgumentException";
            return false;
        }

        if ("/".equals(path)) {
            System.out.println("root arg error during create dir");
            error = "";
            return false;
        }

        String totalPath = String.format("%s/%s", this.rootPath, path);
        totalPath = Util.sanitizePath(totalPath);

        File file = new File(totalPath);
        if (file.exists()) {
            System.out.println("file exist error during create dir");
            System.out.println("current state of files " + this.files);
            return false;
        }

        try {
            createFile(totalPath);
            return true;
        } catch (Exception e) {
            System.out.println("error in creating dir due to " + e.getMessage());
        }

        return false;
    }

    private void exceptionHanler(HttpServletResponse w, String exceptionType, String exceptionInfo, int status) throws IOException {
        ExceptionReturn exceptionReturn = new ExceptionReturn(exceptionType, exceptionInfo);
        w.setHeader("Content-Type","application/json");
        w.setStatus(status);
        w.getWriter().write(gson.toJson(exceptionReturn));
    }

    // /storage_create endpoint for creating a new file
    public void storageCreateHandler(HttpServletResponse w, HttpServletRequest r) throws IOException {
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        PathRequest pathRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathRequest = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, "BadRequestException", "Bad Request", HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!pathRequest.path.isEmpty()) {
            pathRequest.path = Util.sanitizePath(pathRequest.path);
        }

        boolean success = createFileMain(pathRequest.path);
        if (!success) {
            exceptionHanler(w, "IOException", "IO Exception", HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        BooleanReturn response = new BooleanReturn(success);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));

    }

    private boolean deleteFileMain(String path) {

        if (path.isEmpty()) {
            System.out.println("empty arg error during delete dir");
            return false;
        }

        if ("/".equals(path)) {
            System.out.println("root arg error during delete dir");
            return false;
        }

        String totalPath = String.format("%s/%s", this.rootPath, path);
        totalPath = Util.sanitizePath(totalPath);

        File file = new File(totalPath);
        if (!file.exists()) {
            System.out.println("file does not exist error during delete dir");
            System.out.println("current state of files " + this.files);
            return false;
        }

        try {
            file.delete();
            return true;
        } catch (Exception e) {
            System.out.println("error in deleting dir due to " + e.getMessage());
        }

        return false;
    }
    // /storage_delete endpoint for deleting a file
    public void storageDeleteHandler(HttpServletResponse w, HttpServletRequest r) throws IOException {
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        PathRequest pathRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathRequest = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, "BadRequestException", "Bad Request", HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!pathRequest.path.isEmpty()) {
            pathRequest.path = Util.sanitizePath(pathRequest.path);
            System.out.println("Santitized to dir"+ pathRequest.path);
        }

        boolean success = deleteFileMain(pathRequest.path);
        if (!success) {
            exceptionHanler(w, "IllegalArgumentException", "Illegal argument", HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        BooleanReturn response = new BooleanReturn(success);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }

    // listFilesRelativePaths takes a directory path and returns a slice of relative file paths
    private List<String> listFilesRelativePaths(String rootDir) {
        List<String> files= new ArrayList<>();

        try {
            File dir = new File(rootDir);
            File[] filesInDir = dir.listFiles();
            if (filesInDir == null) {
                return files;
            }
            for (File file : filesInDir) {
                if (file.isDirectory()) {
                    continue;
                }
                String relativePath = file.getPath().replace(rootDir, "");
                files.add("/" + relativePath);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Files in dir " + files);        
        return files;
    }

    // deleteFiles takes a directory path and a slice of relative file paths to delete
    private boolean deleteFiles(String rootDir, List<String> files) {
        for (String file : files) {
            File fileToDelete = new File(rootDir + file);
            if (!fileToDelete.delete()) {
                System.out.println("Failed to delete file " + file);
                return false;
            }
        }
        return true;
    }

    //TS: Confirm the logic here
    private void deleteEmptyDirFiles(String rootDir) {
        // Delete files from directories that are empty or become empty after file deletion.
        File rootDirFile = new File(rootDir);
        File[] filesInDir = rootDirFile.listFiles();
        if (filesInDir == null) {
            return;
        }
        for (File fileInDir : filesInDir) {
            if (fileInDir.isDirectory()) {
                File[] filesInSubDir = fileInDir.listFiles();
                if (filesInSubDir == null || filesInSubDir.length == 0) {
                    if (!fileInDir.delete()) {
                        System.out.println("Failed to delete empty directory " + fileInDir);
                    }
                }
            }
        }
    }   

    public SuccessfulRegistrationResponse makeCallToNamingServer() {
        String url = String.format("http://127.0.0.1:%d/register", this.naminServerPort);
        RegisterRequest body = new RegisterRequest("127.0.0.1", this.clientPort, this.commandPort, this.files.toArray(new String[this.files.size()]));

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return gson.fromJson(response.body(), SuccessfulRegistrationResponse.class);
            } else {
                System.err.println("Error from naming server: " + response.statusCode());
                return new SuccessfulRegistrationResponse();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new SuccessfulRegistrationResponse();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return new SuccessfulRegistrationResponse();
        }

    }

    public void registerStorageServer() {
        System.out.println("Registering with storage server, state"+ this);
        SuccessfulRegistrationResponse response = makeCallToNamingServer();
        if (response != null) {
            System.out.println("Successfully registered, deleting files "+ response.files);
            // do pruning of the listed files
            deleteFiles(this.rootPath, List.of(response.files));
            deleteEmptyDirFiles(this.rootPath);
            this.files = listFilesRelativePaths(this.rootPath);

            System.out.println("New state of storage server, state"+ this);
        }
    }

    private long findSize(String path){
        path = Util.sanitizePath(path);

        if(path.isEmpty()){
            return 0;
        }

        if("/".equals(path)){
            return 0;
        }

        File file = new File(path);
        if(file.isDirectory()){
            System.out.println(path+" is a directory");
            return -1;
        }
        return file.length();
    }
    
    public void storageSizeHandler(HttpServletResponse w, HttpServletRequest r) throws IOException {
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        PathRequest pathRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            pathRequest = gson.fromJson(requestBody, PathRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, "BadRequestException", "Bad Request", HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!pathRequest.path.isEmpty()) {
            pathRequest.path = Util.sanitizePath(pathRequest.path);
            System.out.println("Santitized to dir"+ pathRequest.path);
        }

        long size = findSize(String.format("%s/%s", this.rootPath, pathRequest.path));
        if (size == -1) {
            exceptionHanler(w, "IllegalArgumentException", "Illegal argument", HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        SizeReturn response = new SizeReturn(size);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }

    public void fileWrite(String path, long offset, String data) throws IOException{
        path = Util.sanitizePath(path);
        if(path.equals(this.rootPath)){
            throw new IllegalArgumentException("Root path is not allowed");
        }
        if(offset < 0){
            throw new IndexOutOfBoundsException("Offset cannot be < 0");
        }

        File file = new File(path);
        if(file.isDirectory()){
            throw new IllegalArgumentException("Path is a directory");
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(path, true)) {
            if (fileOutputStream.getChannel().size() < offset) {
                throw new IndexOutOfBoundsException("Offset is greater than the file size");
            }
            byte[] dataBytes = Base64.getDecoder().decode(data);
            fileOutputStream.getChannel().position(offset);
            fileOutputStream.write(dataBytes);
        } catch (IOException e) {
            throw new IOException("IOException while writing to file", e);
        }
    }

    public void storageWriteHandler(HttpServletResponse w, HttpServletRequest r) throws IOException {
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        WriteRequest request = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            request = gson.fromJson(requestBody, WriteRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, "BadRequestException", "Bad Request", HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!request.path.isEmpty()) {
            request.path = Util.sanitizePath(request.path);
            System.out.println("Santitized to dir"+ request.path);
        }
        try {
            fileWrite(String.format("%s/%s", this.rootPath, request.path), request.offset, request.data);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, e.getClass().getSimpleName(), e.getMessage(), HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        BooleanReturn response = new BooleanReturn(true);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }

    private String fileRead(String path, long offset, long length) throws IOException{
        path = Util.sanitizePath(path);
        if(path.equals(this.rootPath)){
            throw new IllegalArgumentException("Root path is not allowed");
        }
        if(offset < 0){
            throw new IndexOutOfBoundsException("Offset cannot be < 0");
        }
        if(length < 0){
            throw new IndexOutOfBoundsException("Length cannot be < 0");
        }

        File file = new File(path);
        if(file.isDirectory()){
            throw new IllegalArgumentException("Path is a directory");
        }
        try (FileInputStream fileInputStream = new FileInputStream(path)) {
            if (fileInputStream.getChannel().size() < offset) {
                throw new IndexOutOfBoundsException("Offset is greater than the file size");
            }
            byte[] dataBytes = new byte[(int) length];
            fileInputStream.getChannel().position(offset);
            fileInputStream.read(dataBytes);
            return Base64.getEncoder().encodeToString(dataBytes);
        } catch (IOException e) {
            throw new IOException("IOException while reading from file", e);
        }
    }


    public void storageReadHandler(HttpServletResponse w, HttpServletRequest r) throws IOException {
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        ReadRequest request = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            request = gson.fromJson(requestBody, ReadRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, "BadRequestException", "Bad Request", HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!request.path.isEmpty()) {
            request.path = Util.sanitizePath(request.path);
            System.out.println("Santitized to dir"+ request.path);
        }
        try {
            String data = fileRead(String.format("%s/%s", this.rootPath, request.path), request.offset, request.length);
            DataReturn response = new DataReturn(data);
            w.setContentType("application/json");
            w.setStatus(HttpServletResponse.SC_OK);
            w.getWriter().write(gson.toJson(response));
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, e.getClass().getSimpleName(), e.getMessage(), HttpServletResponse.SC_NOT_FOUND);
            return;
        }
    }

    private SizeReturn sizeCallToStorageServer(String url, String path){
        PathRequest pathRequest = new PathRequest(path);
        System.out.println("Making a size call to storage server for copy"+ url);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(pathRequest)))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return gson.fromJson(response.body(), SizeReturn.class);
            } else {
                System.err.println("Error from naming server: " + response.statusCode());
                return new SizeReturn(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new SizeReturn(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return new SizeReturn(0);
        }
        
    }

    private DataReturn readCallToStorageServer(String url, String path, long offset, long length){
        ReadRequest readRequest = new ReadRequest(path, offset, length);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(readRequest)))
                        .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return gson.fromJson(response.body(), DataReturn.class);
            } else {
                System.err.println("Error from naming server: " + response.statusCode());
                return new DataReturn("");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new DataReturn("");
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return new DataReturn("");
        }
    }

    //Same method as delete call, create call and write call
    private BooleanReturn createCallToStorageServer(String url, String path){
        PathRequest pathRequest = new PathRequest(path);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(pathRequest)))
                        .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return gson.fromJson(response.body(), BooleanReturn.class);
            } else {
                System.err.println("Error from naming server: " + response.statusCode());
                return new BooleanReturn(false);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new BooleanReturn(false);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return new BooleanReturn(false);
        }
    }

    public boolean copyFile(String remotePath, String source, int port){
        if(remotePath.isEmpty()){
            throw new IllegalArgumentException("Remote path is empty");
        }
      
        String sizeUrl = String.format("http://%s:%d/storage_size", source, port);
      
        SizeReturn sizeReturn = sizeCallToStorageServer(sizeUrl, remotePath);
        if(sizeReturn.size == 0){
            throw new IllegalArgumentException("Remote path does not exist");
        }

        String readUrl = String.format("http://%s:%d/storage_read", source, port);
        DataReturn dataReturn = readCallToStorageServer(readUrl, remotePath, 0L, sizeReturn.size);
        if(dataReturn.data.isEmpty()){
            throw new IllegalArgumentException("Remote path does not exist");
        }

        sizeUrl = String.format("http://%s:%d/storage_size", "localhost", this.commandPort);
        sizeReturn = sizeCallToStorageServer(sizeUrl, remotePath);
        if(sizeReturn.size == 0){
            String createUrl = String.format("http://%s:%d/storage_create", "localhost", this.commandPort);
            createCallToStorageServer(createUrl, remotePath);
        } else{
            String deleteUrl = String.format("http://%s:%d/storage_delete", "localhost", this.commandPort);
            createCallToStorageServer(deleteUrl, remotePath);

            String createUrl = String.format("http://%s:%d/storage_create", "localhost", this.commandPort);
            createCallToStorageServer(createUrl, remotePath);
        }

        String writeUrl = String.format("http://%s:%d/storage_write", "localhost", this.commandPort);
        BooleanReturn writeReturn = createCallToStorageServer(writeUrl, remotePath);
        if(!writeReturn.success){
            throw new IllegalArgumentException("Remote path does not exist");
        }
        return true;  
    }

    public void storageCopyHandler(HttpServletResponse w, HttpServletRequest r) throws IOException {
        if (r.getMethod() != "POST") {
            exceptionHanler(w, "MethodNotAllowedException", "Method not allowed", HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        CopyRequest copyFileRequest = null;
        try {
            BufferedReader reader = r.getReader();
            String requestBody = reader.lines().collect(Collectors.joining("\n"));
            copyFileRequest = gson.fromJson(requestBody, CopyRequest.class);
        } catch (Exception e) {
            e.printStackTrace();
            exceptionHanler(w, "BadRequestException", "Bad Request", HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!copyFileRequest.path.isEmpty()) {
            copyFileRequest.path = Util.sanitizePath(copyFileRequest.path);
            System.out.println("Santitized to dir"+ copyFileRequest.path);
        }

        boolean success = copyFile(copyFileRequest.path, copyFileRequest.server_ip, copyFileRequest.server_port);
        if (!success) {
            exceptionHanler(w, "IllegalArgumentException", "Illegal argument", HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        BooleanReturn response = new BooleanReturn(success);
        w.setContentType("application/json");
        w.setStatus(HttpServletResponse.SC_OK);
        w.getWriter().write(gson.toJson(response));
    }


    public static void main(String[] args) {
        
    }
    

 
    
}
