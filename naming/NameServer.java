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

import common.ErrorRegistrationResponse;
import common.RegisterRequest;
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

    private void createFile(String path) throws FileNotFoundException{
        if(path== null || path == ""){
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

        if (!parentNode.isDir()){
            throw new FileNotFoundException();
        }

        this.fileSystem.addFile(path, false, 0);
        // Add the file to the storage server
        Util.callStorageServer(String.format("http://127.0.0.1:%d/storage_create", this.registeredNodes.get(0)), path);
    }

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

}
