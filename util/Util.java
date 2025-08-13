package util;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;

import common.CopyRequest;
import common.PathRequest;
import naming.TreeNode;

public class Util {
    public static final Gson gson = new Gson();
    public static void handleReplication(TreeNode current, List<Integer> storageNodes, String path, List<Integer> client_ports,
            Map<Integer, Integer> port_map) {
        Integer sourceNode = current.sourcePorts.get(0);
        Map<Integer, Boolean> sourceMap = new HashMap<>();
        for (Integer node : current.sourcePorts) {
            sourceMap.put(node, true);
        }

        for (Integer node : client_ports) {
            if (!sourceMap.containsKey(node)) {
                Integer port = port_map.get(node);
                callStorageServerToReplicate(String.format("http://127.0.0.1:%d/storage_copy", port), path, sourceNode);
                current.sourcePorts.add(node);
                break;
            }
        }

    }

    public static void callStorageServerToReplicate(String format, String path, Integer sourceNode) {
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

    public static void handleReplicaDeletion(TreeNode current, String path, Map<Integer, Integer> port_map) {
        List<Integer> node_list = current.sourcePorts;
        if (node_list.size() == 1) {
            return;
        }

        current.sourcePorts = List.of(node_list.get(0));
        System.out.println("Deleting copies on:");
        for (int i = 1; i < node_list.size(); i++) {
            System.out.println(node_list.get(i));
            callStorageServer(String.format("http://127.0.0.1:%d/storage_delete", port_map.get(node_list.get(i))),
                    path);
            System.out.println("Deleted");
        }

    }

    public static void callStorageServer(String url, String path) {
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


    public static String sanitizePath(String filePath) {
        String cleanPath = filePath.trim();
        if (cleanPath == ".") {
            return "/";
        }
        return cleanPath;
    }


}
