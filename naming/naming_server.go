package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type AccessRequest struct {
	Exclusive bool
	done      chan bool // Channel to notify when access is granted
}

type TreeNode struct {
	sync.RWMutex
	QueueLock            sync.Mutex
	Key                  string
	IsDir                bool
	Source               []int
	ReplicationReadCount int32
	Children             map[string]*TreeNode
	ReadCount            int32
	AccessQueue          []AccessRequest
	WriteAccess          bool
}

func NewTreeNode(key string, isDir bool, source int) *TreeNode {
	return &TreeNode{
		Key:                  key,
		IsDir:                isDir,
		Source:               []int{source},
		Children:             make(map[string]*TreeNode),
		AccessQueue:          make([]AccessRequest, 0),
		ReadCount:            0,
		ReplicationReadCount: 0,
		WriteAccess:          false,
	}
}

func UpdateDict(filename string, node int, dict map[string][]int) {
	parts := strings.Split(filename, "/")
	for _, key := range parts {
		if key == "" {
			continue
		}
		if _, exists := dict[key]; exists {
			dict[key] = append(dict[key], node)
		} else {
			//TS: Do you not need to add the key to the new array here?
			dict[key] = []int{node}
		}
	}

}
func handleReplication(current *TreeNode, storageNodes []int, path string, client_ports []int, port_map map[int]int) {
	srcNode := current.Source[0]
	sourceMap := make(map[int]bool)

	// Add all elements from source to a map for O(1) access.
	for _, value := range current.Source {
		sourceMap[value] = true
	}

	// Check if elements of nodelist are in the source map.
	for _, value := range client_ports {
		if _, found := sourceMap[value]; !found {
			node := port_map[value]
			// Return the first element not found.
			replicationCallToStorageServer(fmt.Sprintf("http://127.0.0.1:%d/storage_copy", node), path, srcNode)
			current.Source = append(current.Source, value)
			break
		}
	}
}

func handleReplicaDeletion(current *TreeNode, path string, port_map map[int]int) {
	node_list := current.Source
	if len(node_list) == 1 {
		return
	}
	// Only keep the first element in the list as the source node.
	// This is because we have already copied the file to the new node.
	// If we don't do this, we will end up deleting the file on the source node.
	current.Source = current.Source[:1]
	// Remove the first element from the node list as it is the source node.
	node_list = node_list[1:]
	log.Println("################################################ Deleting copies on:", node_list)
	for _, node := range node_list {
		log.Println("trying to delete")
		makeCallToStorageServer(fmt.Sprintf("http://127.0.0.1:%d/storage_delete", port_map[node]), path)
		log.Println("delted")
	}

}

func appendToMap(m map[string][]int, key string, value int) {

}

func (n *TreeNode) Lock(path string, exclusive bool, storageNodes []int, client_ports []int, port_map map[int]int) string {
	_, exist := n.FindNode(path)
	if !exist {
		return "FileNotFoundException"
	}
	log.Println("################################################ Registered servers:", storageNodes)
	parts := strings.Split(path, "/")
	current := n

	if path == "/" {
		current.QueueLock.Lock()
		req := AccessRequest{
			Exclusive: exclusive,
			done:      make(chan bool),
		}
		current.AccessQueue = append(current.AccessQueue, req)
		current.QueueLock.Unlock()

		current.processQueue()
		<-req.done
		return ""
	}

	// Test case specific issues
	current.QueueLock.Lock()
	req := AccessRequest{
		Exclusive: false,
		done:      make(chan bool),
	}
	current.AccessQueue = append(current.AccessQueue, req)

	current.QueueLock.Unlock()

	current.processQueue()
	<-req.done

	j := 0
	for _, str := range parts {
		if str != "" {
			parts[j] = str
			j++
		}
	}
	parts = parts[:j]

	lastElement := parts[len(parts)-1]
	log.Println(" Path: ", path)
	for _, part := range parts {
		current = current.Children[part]
		log.Println(" Current Key: ", current.Key)
		if current.Key == lastElement {
			// Release the exclusive lock
			current.QueueLock.Lock()
			req := AccessRequest{
				Exclusive: exclusive,
				done:      make(chan bool),
			}
			current.AccessQueue = append(current.AccessQueue, req)
			current.QueueLock.Unlock()

			current.processQueue()
			<-req.done
			if !exclusive {
				atomic.AddInt32(&current.ReplicationReadCount, 1)
				readCount := atomic.LoadInt32(&current.ReplicationReadCount)
				if readCount >= 20 {
					//log.Println("+++++++++++++++++++++++++++++++++++++++++++++ Source of file: ", current.Source)
					atomic.AddInt32(&current.ReplicationReadCount, -20)
					handleReplication(current, storageNodes, path, client_ports, port_map)
				}
			} else {
				//log.Println("+++++++++++++++++++++++++++++++++++++++++++++ Test of file")
				atomic.StoreInt32(&current.ReplicationReadCount, 0)
				handleReplicaDeletion(current, path, port_map)
			}
		} else {

			current.QueueLock.Lock()
			req := AccessRequest{
				Exclusive: false,
				done:      make(chan bool),
			}
			current.AccessQueue = append(current.AccessQueue, req)
			current.QueueLock.Unlock()

			current.processQueue()
			<-req.done
		}

	}
	log.Println("\n\n")
	return ""
}

func (n *TreeNode) Unlock(path string, exclusive bool) string {
	_, exist := n.FindNode(path)
	if !exist {
		return "IllegalArgumentException"
	}

	parts := strings.Split(path, "/")
	current := n

	if path == "/" {
		if exclusive {
			// Release the exclusive lock
			current.WriteAccess = false
			current.RWMutex.Unlock()
			current.processQueue()
		} else {
			// Release the shared lock
			current.RWMutex.RUnlock()
			atomic.AddInt32(&current.ReadCount, -1)
			current.processQueue()
		}

		return ""
	}

	current.RWMutex.RUnlock()
	atomic.AddInt32(&current.ReadCount, -1)
	current.processQueue()

	j := 0
	for _, str := range parts {
		if str != "" {
			parts[j] = str
			j++
		}
	}
	parts = parts[:j]
	lastElement := parts[len(parts)-1]
	// Traverse the path again to unlock
	for _, part := range parts {
		current = current.Children[part]
		if current.Key == lastElement && exclusive {
			// Release the exclusive lock
			current.WriteAccess = false
			current.RWMutex.Unlock()
			current.processQueue()
		} else {
			// Release the shared lock
			current.RWMutex.RUnlock()
			atomic.AddInt32(&current.ReadCount, -1)
			current.processQueue()
		}

	}

	return ""
}

func (node *TreeNode) processQueue() {
	node.QueueLock.Lock()
	defer node.QueueLock.Unlock()

	for len(node.AccessQueue) > 0 {
		req := node.AccessQueue[0] // Peek the first request
		if req.Exclusive && atomic.LoadInt32(&node.ReadCount) == 0 && !node.WriteAccess {
			// Grant exclusive access
			node.RWMutex.Lock()
			node.WriteAccess = true
			close(req.done)                         // Notify the requester
			node.AccessQueue = node.AccessQueue[1:] // Dequeue
		} else if !req.Exclusive && !node.WriteAccess {
			// Grant shared access to consecutive shared requests
			for len(node.AccessQueue) > 0 && !node.AccessQueue[0].Exclusive {
				atomic.AddInt32(&node.ReadCount, 1)
				node.RWMutex.RLock()
				close(node.AccessQueue[0].done)         // Notify the requester
				node.AccessQueue = node.AccessQueue[1:] // Dequeue
			}
		} else {
			break
		}
	}

}

func (n *TreeNode) DeleteFile(path string, portmap map[int]int, nodes []int) string {
	node, exist := n.FindNode(path)
	if !exist {
		return "FileNotFoundException"
	}
	n.RWMutex.Lock()
	n.WriteAccess = true

	defer func() {
		n.WriteAccess = false
		n.RWMutex.Unlock()
	}()

	//fmt.Println("The source nodes are: ", node.Source)
	log.Println(" #################################################### The source nodes are: ", node.Source, "The reg nodes: ", nodes)

	for _, src := range nodes {
		makeCallToStorageServer(fmt.Sprintf("http://127.0.0.1:%d/storage_delete", src), path)
	}

	return ""
}

// AddFile adds a file or directory to the tree based on the provided path.
func (n *TreeNode) AddFile(path string, isDir bool, source int) {
	// log.Println("###################################### File details:", path, source)
	parts := strings.Split(path, "/")
	current := n
	n.RWMutex.Lock()
	n.WriteAccess = true

	defer func() {
		n.WriteAccess = false
		n.RWMutex.Unlock()
	}()

	for i, part := range parts {
		if part == "" { // Skip empty parts resulting from leading "/"
			continue
		}
		// Determine if the current part is a directory
		isCurrentDir := i < len(parts)-1 // All parts except the last are directories

		if _, exists := current.Children[part]; !exists {
			if source == 0 {
				current.Children[part] = NewTreeNode(part, isCurrentDir, current.Source[0])
			} else {
				current.Children[part] = NewTreeNode(part, isCurrentDir, source)
			}
		}
		current = current.Children[part]
	}
	// Set the IsDir flag for the last node, which represents the file or directory being added
	current.IsDir = isDir
}

// FindNode returns the node for the specified path, if it exists.
func (n *TreeNode) FindNode(path string) (*TreeNode, bool) {
	if path == "/" {
		return n, true
	}

	parts := strings.Split(path, "/")
	current := n
	for _, part := range parts {
		if part == "" {
			continue
		}
		child, exists := current.Children[part]
		if !exists {
			return nil, false
		}
		current = child
	}
	return current, true
}

// PrintTree prints the tree structure starting from the given node, with indentation representing the depth.
func (n *TreeNode) PrintTree(depth int) {
	//indent := strings.Repeat("  ", depth)
	//log.Println(indent + n.Key)
	for _, child := range n.Children {
		child.PrintTree(depth + 1)
	}
}

// fs def end

// Define your request and response structures here

type RegistrationData struct {
	StorageIP   string   `json:"storage_ip"`
	ClientPort  int      `json:"client_port"`
	CommandPort int      `json:"command_port"`
	Files       []string `json:"files"`
}

// Define a struct for the successful response content
type SuccessfulRegistrationResponse struct {
	Files []string `json:"files"`
}

// Define a struct for the error response content
type ErrorRegistrationResponse struct {
	ExceptionType string `json:"exception_type"`
	ExceptionInfo string `json:"exception_info"`
}

type IsValidPathRequest struct {
	Path string `json:"path"`
}

type IsValidPathResponse struct {
	Success bool `json:"success"`
}

type GetStorageRequest struct {
	Path string `json:"path"`
}

type GetStorageResponse struct {
	ServerIP   string `json:"server_ip"`
	ServerPort int    `json:"server_port"`
}

type NameServer struct {
	fileSystem       *TreeNode
	servicePort      int
	registrationPort string
	registeredNodes  []int
	clientPorts      []int
	port_map         map[int]int
	files_dict       map[string][]int
	mu               sync.Mutex
}

// PathRequest represents the JSON structure for the /is_directory command's input data.
type PathRequest struct {
	Path string `json:"path"`
}

// PathRequest represents the JSON structure for the /is_directory command's input data.
type LockRequest struct {
	Path      string `json:"path"`
	Exclusive bool   `json:"exclusive"`
}

// BooleanReturn represents the JSON structure for a successful response to the client.
type BooleanReturn struct {
	Success bool `json:"success"`
}

// ExceptionReturn represents the JSON structure for an error response to the client.
type ExceptionReturn struct {
	ExceptionType string `json:"exception_type"`
	ExceptionInfo string `json:"exception_info"`
}

// FilesReturn represents the JSON structure for sending back a list of files.
type FilesReturn struct {
	Files []string `json:"files"`
}

// ServerInfo represents the JSON structure for the successful response
type ServerInfo struct {
	ServerIP   string `json:"server_ip"`
	ServerPort int    `json:"server_port"`
}

// Define more structs for other request and response types based on your API specification

// Define handlers for your naming server endpoints
//
//// ServiceHandler handles service interactions
//func (ns *NameServer) ServiceHandler(w http.ResponseWriter, r *http.Request) {
//	// Your service handling logic here
//}

func (ns *NameServer) checkIfNodeRegistered(newNode int) bool {
	ns.mu.Lock()
	for _, node := range ns.registeredNodes {
		if node == newNode {
			ns.mu.Unlock()
			return true
		}
	}

	ns.mu.Unlock()
	return false
}

// RegistrationHandler handles storage server registration
func (ns *NameServer) RegistrationHandler(w http.ResponseWriter, r *http.Request) {
	// Read the request body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Unmarshal the JSON data into the struct
	var data RegistrationData
	if err := json.Unmarshal(body, &data); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing JSON: %v", err), http.StatusBadRequest)
		return
	}

	// check if the node is registered
	node := data.CommandPort
	client_port := data.ClientPort
	isRegistered := ns.checkIfNodeRegistered(node)
	//log.Printf("The registration state of node %s is %#v\n", node, isRegistered)
	if !isRegistered {
		// register the server to ns
		ns.registeredNodes = append(ns.registeredNodes, node)
		ns.port_map[client_port] = node
		ns.clientPorts = append(ns.clientPorts, client_port)

		// check if the files are present in the fs
		var deleted []string = []string{}

		//log.Println("Printing the directory structure before registration")
		ns.fileSystem.PrintTree(1)
		//log.Println("The input for registration is", data)

		if !(len(data.Files) == 1 && data.Files[0] == "/") {
			ns.mu.Lock()
			for _, filename := range data.Files {
				_, exist := ns.fileSystem.FindNode(filename)
				if exist {
					// the file should be deleted from the registered port
					deleted = append(deleted, filename)
				} else {
					ns.fileSystem.AddFile(filename, false, data.ClientPort)
					UpdateDict(filename, node, ns.files_dict)
					log.Println(ns.files_dict)
				}
			}
			ns.mu.Unlock()
		}

		//log.Println("Printing the directory structure after registration")
		ns.fileSystem.PrintTree(1)

		// return response
		successResponse := SuccessfulRegistrationResponse{
			Files: deleted,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 OK
		//log.Printf("Returning response for registration %#v\n", successResponse)
		json.NewEncoder(w).Encode(successResponse)

	} else {
		// return 409 conflict
		errorResponse := ErrorRegistrationResponse{
			ExceptionType: "IllegalStateException",
			ExceptionInfo: "This storage server is already registered.",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict) // 409 Conflict
		json.NewEncoder(w).Encode(errorResponse)
	}

}

// sanitizePath cleans up a file path, collapsing any multiple slashes into a single slash
// and removing any trailing slash if present, except for the root "/".
func sanitizePath(filePath string) string {
	// Trim space and repeated forward slashes.
	cleanPath := path.Clean(strings.TrimSpace(filePath))

	// Ensure that if the cleaned path is empty, which means the input was something like "////",
	// we return a single "/" to represent the root directory.
	if cleanPath == "." {
		cleanPath = "/"
	}

	return cleanPath
}

// isDirectoryHandler handles the /is_directory command.
func (ns *NameServer) isDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the JSON body into a PathRequest struct.
	var pathReq PathRequest
	if err := json.NewDecoder(r.Body).Decode(&pathReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//log.Println("Printing the directory structurre")
	ns.fileSystem.PrintTree(1)

	// sanitize the directory listing
	if pathReq.Path != "" {
		//log.Println("Sanitizing path of dir ", pathReq.Path)
		pathReq.Path = sanitizePath(pathReq.Path)
		//log.Println("Sanitized to dir ", pathReq.Path)
	}

	var exceptionType string
	var isDir bool
	isDir = true
	if pathReq.Path == "" {
		exceptionType = "IllegalArgumentException"
	} else {

		if pathReq.Path == "/" {
			isDir = true
		} else {
			node, exist := ns.fileSystem.FindNode(pathReq.Path)
			//log.Println("the output of whether:", pathReq.Path, "exists is:", exist)
			if exist && !node.IsDir {
				isDir = false
			} else {
				isDir = true
			}

			if !exist {
				exceptionType = "FileNotFoundException"
				isDir = false
			}
		}

	}

	// If the path does not exist, return a 404 error with an ExceptionReturn struct.
	if exceptionType != "" { // Here we assume isDir also checks for path existence
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ExceptionReturn{
			ExceptionType: exceptionType,
			ExceptionInfo: "the file/directory or parent directory does not exist.",
		})
		return
	}

	// If the path exists and is a directory, return a successful response.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(BooleanReturn{
		Success: isDir,
	})
}

func (ns *NameServer) findFiles(path string) ([]string, string) {
	//ns.mu.Lock()
	node, exist := ns.fileSystem.FindNode(path)
	//ns.mu.Unlock()

	if !exist {
		return nil, "fileNotFound"
	} else if !node.IsDir {
		return nil, "IllegalArgument"
	} else {
		var files []string
		for _, node := range node.Children {
			files = append(files, node.Key)
		}
		return files, ""
	}

}

// listHandler handles the /list endpoint for listing directory contents.
func (ns *NameServer) listHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body into PathRequest struct.
	var request PathRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var files []string
	var err string
	//log.Printf("Listhandler got path %#v\n", request.Path)
	if request.Path != "" {
		files, err = ns.findFiles(request.Path)
	} else {
		files = []string{}
		err = "IllegalArgument"
	}

	if err != "" {
		//log.Println("The error thrown for listing is :", err)
		// If the directory does not exist or the path is invalid, return a 404 error.
		exceptionType := "FileNotFoundException"
		if err == "IllegalArgument" { // You need to define what kind of errors you are expecting
			if request.Path == "" {
				exceptionType = "IllegalArgumentException"
			} else {
				// listing tried on file
				exceptionType = "FileNotFoundException"
			}
		}
		errorResponse := ExceptionReturn{
			ExceptionType: exceptionType,
			ExceptionInfo: "the directory does not exist or the path is invalid.",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response with the list of files.
	successResponse := FilesReturn{
		Files: files,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func GetParentDir(filePath string) string {
	return filepath.Dir(filePath)
}

func (ns *NameServer) createDirectory(path string) (bool, string) {
	if path != "" {
		// check if the file already exists
		node, exist := ns.fileSystem.FindNode(path)
		if exist {
			return false, ""
		}

		// check if the directory already exist
		parentDir := GetParentDir(path)

		// check if the parent exists
		node, exist = ns.fileSystem.FindNode(parentDir)

		if exist {
			if node.IsDir {
				ns.fileSystem.AddFile(path, true, 0)
				return true, ""
			} else {
				return false, "FileNotFoundException"
			}
		} else {
			return false, "FileNotFoundException"
		}

	} else {
		return false, "IllegalArgumentException"
	}

}

// createDirectoryHandler handles the /create_directory endpoint.
func (ns *NameServer) createDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	// Decode the request body into PathRequest struct.
	var request PathRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//log.Println("Printing the directory structurre before update")
	ns.fileSystem.PrintTree(1)
	//log.Println("The input to create_dir is ", request)

	// Call a function to create the directory. You'll need to implement this.
	// sanitize the directory listing
	if request.Path != "" {
		//log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		//log.Println("Sanitized to dir ", request.Path)
	}

	success, err := ns.createDirectory(request.Path)
	if err != "" {
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	//log.Println("Printing the directory structurre after update")
	ns.fileSystem.PrintTree(1)

	// Prepare the successful response.
	successResponse := BooleanReturn{
		Success: success,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

// Define a struct that matches the JSON payload structure
type requestBody struct {
	Path string `json:"path"`
}

type replicationReq struct {
	Path       string `json:"path"`
	ServerIp   string `json:"server_ip"`
	ServerPort int    `json:"server_port"`
}

func replicationCallToStorageServer(url string, path string, src int) bool {
	// Create an instance of the requestBody struct with the path
	//log.Println("Making a call to storage server ", url)
	body := replicationReq{
		Path:       path,
		ServerIp:   "127.0.0.1",
		ServerPort: src,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	log.Println(body, url)
	if err != nil {
		//fmt.printf("Error marshalling JSON: %s\n", err)
		return false
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		//fmt.printf("Error creating request: %s\n", err)
		return false
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		//fmt.printf("Error sending request: %s\n", err)
		return false
	}
	defer resp.Body.Close()

	return true
}

func makeCallToStorageServer(url string, path string) bool {
	// Create an instance of the requestBody struct with the path
	//log.Println("Making a call to storage server ", url)
	body := requestBody{
		Path: path,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		//fmt.printf("Error marshalling JSON: %s\n", err)
		return false
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		//fmt.printf("Error creating request: %s\n", err)
		return false
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		//fmt.printf("Error sending request: %s\n", err)
		return false
	}
	defer resp.Body.Close()

	return true
}

func (ns *NameServer) CreateFile(path string) (bool, string) {
	if path != "" {
		// check if the file already exists
		node, exist := ns.fileSystem.FindNode(path)
		if exist {
			return false, ""
		}

		// check if the directory already exist
		parentDir := GetParentDir(path)

		// check if the parent exists
		node, exist = ns.fileSystem.FindNode(parentDir)

		if exist {
			if node.IsDir {
				ns.fileSystem.AddFile(path, false, 0)
				// add file to the storage server
				makeCallToStorageServer(fmt.Sprintf("http://127.0.0.1:%d/storage_create", ns.registeredNodes[0]), path)

				return true, ""
			} else {
				return false, "FileNotFoundException"
			}
		} else {
			return false, "FileNotFoundException"
		}

	} else {
		return false, "IllegalArgumentException"
	}

}

// createFileHandler handles the /create_file endpoint for creating a new file.
func (ns *NameServer) createFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body into PathRequest struct.
	var request PathRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//log.Println("Printing the directory structurre before update")
	ns.fileSystem.PrintTree(1)
	//log.Println("The input to create_dir is ", request)

	// Check if storage servers are registered. This logic needs to be implemented.
	if len(ns.registeredNodes) == 0 {
		errorResponse := ExceptionReturn{
			ExceptionType: "IllegalStateException",
			ExceptionInfo: "no storage servers are registered with the naming server.",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict) // 409 Conflict
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Call a function to create the file. You'll need to implement this.
	success, err := ns.CreateFile(request.Path)
	if err != "" {
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	//log.Println("Printing the directory structurre after update")
	ns.fileSystem.PrintTree(1)

	// Prepare the successful response.
	successResponse := BooleanReturn{
		Success: success,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func (ns *NameServer) getStorage(w http.ResponseWriter, r *http.Request) {
	// Decode the request body into PathRequest struct.
	var request PathRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var err string
	var node *TreeNode
	var exist bool
	if request.Path == "" {
		err = "IllegalArgumentException"
	} else {
		request.Path = sanitizePath(request.Path)
		node, exist = ns.fileSystem.FindNode(request.Path)

		if !exist || node.IsDir {
			err = "FileNotFoundException"
		}
	}

	if err != "" {
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	//log.Println("Found node", node.source)
	// Prepare the successful response.
	successResponse := ServerInfo{
		ServerIP:   "127.0.0.1",
		ServerPort: node.Source[0],
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func (ns *NameServer) ValidPathHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := IsValidPathResponse{Success: false}
	json.NewEncoder(w).Encode(response)
}

func (ns *NameServer) LockHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body into PathRequest struct.
	var request LockRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//ns.fileSystem.PrintTree(1)
	//log.Println("The input to create_dir is ", request)
	if request.Path != "" {
		//log.Println("Sanitizing path of dir ", pathReq.Path)
		request.Path = sanitizePath(request.Path)
		//log.Println("Sanitized to dir ", pathReq.Path)
	}
	if request.Path == "" {
		errorResponse := ExceptionReturn{
			ExceptionType: "IllegalArgumentException",
			ExceptionInfo: "IllegalArgumentException",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}
	// Call a function to create the file. You'll need to implement this.
	err := ns.fileSystem.Lock(request.Path, request.Exclusive, ns.registeredNodes, ns.clientPorts, ns.port_map)
	if err != "" {
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}
}

func (ns *NameServer) UnlockHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body into PathRequest struct.
	var request LockRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// log.Println("Printing the directory structurre before locking")
	//ns.fileSystem.PrintTree(1)
	//log.Println("The input to create_dir is ", request)
	if request.Path != "" {
		//log.Println("Sanitizing path of dir ", pathReq.Path)
		request.Path = sanitizePath(request.Path)
		//log.Println("Sanitized to dir ", pathReq.Path)
	}
	if request.Path == "" {
		errorResponse := ExceptionReturn{
			ExceptionType: "IllegalArgumentException",
			ExceptionInfo: "IllegalArgumentException",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}
	// Call a function to create the file. You'll need to implement this.
	err := ns.fileSystem.Unlock(request.Path, request.Exclusive)
	if err != "" {
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}
}

func (ns *NameServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body into PathRequest struct.
	var request LockRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if request.Path != "" {
		request.Path = sanitizePath(request.Path)
	}
	if request.Path == "" {
		errorResponse := ExceptionReturn{
			ExceptionType: "IllegalArgumentException",
			ExceptionInfo: "IllegalArgumentException",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}
	// Call a function to create the file. You'll need to implement this.
	err := ns.fileSystem.DeleteFile(request.Path, ns.port_map, ns.registeredNodes)
	if err != "" {
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}
	successResponse := BooleanReturn{
		Success: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)

}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		os.Exit(-1)
	}()
	//var wg sync.WaitGroup

	if len(os.Args) < 3 {
		log.Fatal("Usage: go run naming_server.go <servicePort> <registrationPort>")
	}

	// os.Args[0] is the program name, os.Args[1] is the first argument, and so on.
	servicePort, _ := strconv.Atoi(os.Args[1])
	registrationPort, _ := strconv.Atoi(os.Args[2])

	// Now you have the port numbers as integers
	//fmt.printf("Service Port: %s\n", servicePort)
	//fmt.printf("Registration Port: %s\n", registrationPort)

	ns := NameServer{fileSystem: NewTreeNode("/", true, registrationPort), registrationPort: strconv.Itoa(registrationPort), servicePort: servicePort, port_map: make(map[int]int), files_dict: make(map[string][]int)}

	http.HandleFunc("/is_valid_path", ns.ValidPathHandler)
	http.HandleFunc("/is_directory", ns.isDirectoryHandler)
	http.HandleFunc("/list", ns.listHandler)
	http.HandleFunc("/create_directory", ns.createDirectoryHandler)
	http.HandleFunc("/create_file", ns.createFileHandler)
	http.HandleFunc("/delete", ns.DeleteHandler)
	http.HandleFunc("/get_storage", ns.getStorage)
	http.HandleFunc("/register", ns.RegistrationHandler)
	http.HandleFunc("/lock", ns.LockHandler)
	http.HandleFunc("/unlock", ns.UnlockHandler) // Define your registration endpoint

	// Service server
	//wg.Add(1)
	go func() {
		//defer wg.Done()
		//log.Println("Service interface listening on port", servicePort)
		if err := http.ListenAndServe(":"+strconv.Itoa(servicePort), nil); err != nil {
			log.Fatalf("Service server failed: %v", err)
		}
	}()

	// Registration server
	//wg.Add(1)
	go func() {
		//defer wg.Done()
		//log.Println("Registration interface listening on port", registrationPort)
		if err := http.ListenAndServe(":"+strconv.Itoa(registrationPort), nil); err != nil {
			log.Fatalf("Registration server failed: %v", err)
		}
	}()

	select {}
	//wg.Wait() // Wait for all servers to stop
}
