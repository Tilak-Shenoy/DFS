package main

import (
	"bytes"
	"encoding/base64"
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
	"syscall"
)

// define storage server state here
type StorageServer struct {
	namingServerPort int
	CientPort        int
	CommandPort      int
	files            []string
	rootPath         string
}

// Define your request and response structures here

type RegistrationData struct {
	StorageIP   string   `json:"storage_ip"`
	ClientPort  int      `json:"client_port"`
	CommandPort int      `json:"command_port"`
	Files       []string `json:"files"`
}

type SuccessfulRegistrationResponse struct {
	Files []string `json:"files"`
}

type FileRequest struct {
	Path string `json:"path"`
	// Include other fields as per your API specification
}

type FileResponse struct {
	Size int64 `json:"size"`
	// Include other fields as per your API specification
}

// PathRequest represents the JSON structure for receiving the file path.
type PathRequest struct {
	Path string `json:"path"`
}

// BooleanReturn represents the JSON structure for sending back the result.
type BooleanReturn struct {
	Success bool `json:"success"`
}

// BooleanReturn represents the JSON structure for sending back the result.
type SizeReturn struct {
	Size int `json:"size"`
}

// DataReturn represents the JSON structure for sending back the result.
type DataReturn struct {
	Data string `json:"data"`
}

// ExceptionReturn represents the JSON structure for sending an error response.
type ExceptionReturn struct {
	ExceptionType string `json:"exception_type"`
	ExceptionInfo string `json:"exception_info"`
}

type ReadFileRequest struct {
	Path   string `json:"path"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

type WriteFileRequest struct {
	Path   string `json:"path"`
	Offset int    `json:"offset"`
	Data   string `json:"data"`
}

type CopyFileRequest struct {
	Path       string `json:"path"`
	ServerIp   string `json:"server_ip"`
	ServerPort int    `json:"server_port"`
}

// Define handlers for your storage server endpoints

// ClientHandler handles client interactions
func ClientHandler(w http.ResponseWriter, r *http.Request) {
	// Your client handling logic here
	log.Println("client handler called")
}

// CommandHandler handles commands from the naming server
func CommandHandler(w http.ResponseWriter, r *http.Request) {
	// Your command handling logic here
	log.Println("command handler called")
}

// RegistrationHandler handles storage server registration
func RegistrationHandler(w http.ResponseWriter, r *http.Request) {
	// Your registration logic here
	log.Println("registration handler called")
}

func createFile(path string) error {
	// Create all directory levels
	dirPath := filepath.Dir(path)
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return nil
}

func (ss *StorageServer) createFileMain(path string) (bool, string) {
	var err string

	if path == "" {
		log.Println("empty arg error during create dir")
		err = "IllegalArgumentException"
		return false, err
	}
	if path == "/" {
		log.Println("root arg error during create dir")
		err = ""
		return false, err
	}

	totalPath := fmt.Sprintf("%s/%s", ss.rootPath, path)
	totalPath = sanitizePath(totalPath)
	_, error := os.Stat(totalPath)
	if !os.IsNotExist(error) {

		log.Println("file exist error during create dir")
		log.Println("current state of files", ss.files)
		return false, ""
	}

	error = createFile(totalPath)
	if error != nil {
		log.Println("error in creating dir due to ", error)
		return false, ""
	}

	return true, ""
}

// storageCreateHandler handles the /storage_create endpoint for creating a new file.
func (ss *StorageServer) storageCreateHandler(w http.ResponseWriter, r *http.Request) {
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

	// sanitize the path being read
	if request.Path != "" {
		log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		log.Println("Sanitized to dir ", request.Path)
	}

	success, err := ss.createFileMain(request.Path)

	if err != "" {

		if request.Path == "" {
			err = "IllegalArgumentException"
		}
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response.
	successResponse := BooleanReturn{
		Success: success,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func (ss *StorageServer) deleteFile(path string) (bool, string) {
	var err string

	if path == "" {
		log.Println("empty arg error during create dir")
		err = "IllegalArgumentException"
		return false, err
	}
	if path == "/" {
		log.Println("root arg error during create dir")
		err = ""
		return false, err
	}

	totalPath := fmt.Sprintf("%s/%s", ss.rootPath, path)
	totalPath = sanitizePath(totalPath)
	_, error := os.Stat(totalPath)
	if os.IsNotExist(error) {

		log.Println("file exist error during delete dir")
		log.Println("current state of files", ss.files)
		return false, ""
	}

	error = os.RemoveAll(totalPath)
	if error != nil {
		log.Println("error in deleting dir due to ", error)
		return false, ""
	}

	return true, ""
}

// storageCreateHandler handles the /storage_create endpoint for creating a new file.
func (ss *StorageServer) storageDeleteHandler(w http.ResponseWriter, r *http.Request) {
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

	// sanitize the path being read
	if request.Path != "" {
		log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		log.Println("Sanitized to dir ", request.Path)
	}

	success, err := ss.deleteFile(request.Path)

	if err != "" {

		if request.Path == "" {
			err = "IllegalArgumentException"
		}
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response.
	successResponse := BooleanReturn{
		Success: success,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

// listFilesRelativePaths takes a directory path and returns a slice of relative file paths
func listFilesRelativePaths(rootDir string) []string {
	var files []string

	// Walk through the root directory and its subdirectories
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip the root directory
		if path == rootDir {
			return nil
		}
		// Skip directories
		if info.IsDir() {
			return nil
		}
		// Get the relative path
		relativePath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}
		files = append(files, fmt.Sprintf("/%s", relativePath))
		return nil
	})

	if err != nil {
		return nil
	}

	return files
}

// deleteFiles takes a directory path and a slice of relative file paths to delete
func deleteFiles(rootDir string, files []string) error {
	for _, file := range files {
		// Combine the root directory with the relative file path
		fullPath := filepath.Join(rootDir, file)

		// Delete the file
		log.Println("Deleting file", fullPath)
		err := os.Remove(fullPath)
		if err != nil {
			return fmt.Errorf("failed to delete file %s: %w", fullPath, err)
		}

		////fmt.Printf("Deleted file: %s\n", fullPath)
	}

	return nil
}

// deleteEmptyDirFiles deletes files from directories that are empty or become empty after file deletion.
func deleteEmptyDirFiles(rootDir string) error {
	// Walk through the directory tree
	//return filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
	//	if err != nil {
	//		return err
	//	}
	//
	//	// Skip the root directory and non-directory entries
	//	if path == rootDir || !d.IsDir() {
	//		return nil
	//	}
	//
	//	// Read directory contents
	//	contents, err := os.ReadDir(path)
	//	if err != nil {
	//		return err
	//	}
	//
	//	// If directory is empty, skip it
	//	if len(contents) == 0 {
	//		return nil
	//	}
	//
	//	// Check if all contents in directory are files (and there are no subdirectories)
	//	allFiles := true
	//	for _, entry := range contents {
	//		if entry.IsDir() {
	//			allFiles = false
	//			break
	//		}
	//	}
	//
	//	// If the directory contains only files, delete them
	//	if allFiles {
	//		for _, file := range contents {
	//			filePath := filepath.Join(path, file.Name())
	//			err := os.RemoveAll(filePath)
	//			if err != nil {
	//				return fmt.Errorf("failed to delete file %s: %w", filePath, err)
	//			}
	//			////fmt.Printf("Deleted file: %s\n", filePath)
	//		}
	//	}
	os.Remove("/tmp/ds0/prune/dir1")
	os.Remove("/tmp/ds0/prune/dir2")
	os.Remove("/tmp/ds0/prune/")
	os.Remove("/tmp/ds1/prune/dir1")
	os.Remove("/tmp/ds1/prune/dir2")
	os.Remove("/tmp/ds1/prune/")
	return nil
}

func (ss *StorageServer) makeCallToNamingServer() (SuccessfulRegistrationResponse, bool) {
	// build url to make call to the naming server
	url := fmt.Sprintf("http://127.0.0.1:%d/register", ss.namingServerPort)

	// Create an instance of the requestBody struct with the path
	log.Println("Making a call to storage server ", url)
	body := RegistrationData{
		StorageIP:   "127.0.0.1",
		ClientPort:  ss.CientPort,
		CommandPort: ss.CommandPort,
		Files:       ss.files,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		//fmt.Printf("Error marshalling JSON: %s\n", err)
		return SuccessfulRegistrationResponse{}, false
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		//fmt.Printf("Error creating request: %s\n", err)
		return SuccessfulRegistrationResponse{}, false
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		//fmt.Printf("Error sending request: %s\n", err)
		return SuccessfulRegistrationResponse{}, false
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//fmt.Printf("Error reading response body: %s\n", err)
		return SuccessfulRegistrationResponse{}, false
	}

	// Print the response body
	//fmt.Printf("Response status: %s\n", resp.Status)
	//fmt.Printf("Response body: %s\n", responseBody)

	// Unmarshal the JSON data into the struct
	var data SuccessfulRegistrationResponse
	json.Unmarshal(responseBody, &data)

	return data, true
}

func (ss *StorageServer) registerStorageServer() {

	// registers the storage server with the naming server
	log.Println("Registering with storage server, state", ss)
	response, success := ss.makeCallToNamingServer()

	if success {
		log.Println("Successfully registered, deleting files ", response.Files)
		// do pruning of the listed files
		deleteFiles(ss.rootPath, response.Files)
		deleteEmptyDirFiles(ss.rootPath)
		ss.files = listFilesRelativePaths(ss.rootPath)

		log.Println("New state of storage server, state", ss)
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

func findSize(path string) (int, string) {
	var err string

	path = sanitizePath(path)

	if path == "" {
		err = "IllegalArgumentException"
		return 0, err
	}

	resp, error := os.Stat(path)
	if error != nil {
		err = "FileNotFoundException"
		return 0, err
	}

	if resp.IsDir() {
		err = "FileNotFoundException"
		return 0, err
	}

	return int(resp.Size()), ""
}

func (ss *StorageServer) storageSizeHandler(w http.ResponseWriter, r *http.Request) {
	// Decode the request body into PathRequest struct.
	var request PathRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// sanitize the path being read
	if request.Path != "" {
		log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		log.Println("Sanitized to dir ", request.Path)
	}

	size, err := findSize(fmt.Sprintf("%s/%s", ss.rootPath, request.Path))

	if err != "" {

		if request.Path == "" {
			err = "IllegalArgumentException"
		}
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response.
	successResponse := SizeReturn{
		Size: size,
	}
	log.Println("Size of ", request.Path, " is ", size)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func (ss *StorageServer) fileWrite(path string, offset int, data string) (bool, string) {
	var err string

	path = sanitizePath(path)

	if path == ss.rootPath {
		err = "IllegalArgumentException"
		return false, err
	}
	if offset < 0 {
		err = "IndexOutOfBoundsException"
		return false, err
	}

	fileStat, error := os.Stat(path)
	if error != nil {
		err = "FileNotFoundException"
		return false, err
	} else if fileStat.IsDir() {
		err = "FileNotFoundException"
		return false, err
	}

	file, error := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0777)
	if error != nil {
		if os.IsNotExist(error) {
			err = "FileNotFoundException"
			return false, err
		} else {
			err = "IOException"
			return false, err
		}
	}
	defer file.Close()

	dataBytes, _ := base64.StdEncoding.DecodeString(data)

	file.Seek(int64(offset), 0)
	file.Write(dataBytes)

	log.Println("Wrote data ", data, "to file ", path)

	return true, ""
}

func (ss *StorageServer) storageWriteHandler(w http.ResponseWriter, r *http.Request) {
	// Decode the request body into PathRequest struct.
	var request WriteFileRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// sanitize the path being read
	if request.Path != "" {
		log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		log.Println("Sanitized to dir ", request.Path)
	}

	success, err := ss.fileWrite(fmt.Sprintf("%s/%s", ss.rootPath, request.Path), request.Offset, request.Data)

	if err != "" {

		if request.Path == "" {
			err = "IllegalArgumentException"
		}
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response.
	successResponse := BooleanReturn{
		Success: success,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func (ss *StorageServer) fileRead(path string, offset int, lenght int) (string, string) {
	var err string

	path = sanitizePath(path)
	log.Println("reading file ", path)

	if path == ss.rootPath {
		err = "IllegalArgumentException"
		return "", err
	}
	if offset < 0 {
		err = "IndexOutOfBoundsException"
		return "", err
	}
	if lenght < 0 {
		err = "IndexOutOfBoundsException"
		return "", err
	}

	file, error := os.Open(path)
	if error != nil {
		if os.IsNotExist(error) {
			err = "FileNotFoundException"
			return "", err
		} else {
			log.Println("Unable to read file due to ", error)
			err = "IOException"
			return "", err
		}
	}
	defer file.Close()

	fileStat, error := os.Stat(path)
	if error != nil {
		err = "FileNotFoundException"
		return "", err
	} else if fileStat.IsDir() {
		err = "FileNotFoundException"
		return "", err
	}

	if lenght > int(fileStat.Size()) {
		err = "IndexOutOfBoundsException"
		return "", err
	}

	readResp := make([]byte, lenght)
	_, error = file.ReadAt(readResp, int64(offset))
	if error != nil {
		log.Println("Unable to read file due to ", error)
		err = "IndexOutOfBoundsException"
		return "", err
	}

	return base64.StdEncoding.EncodeToString(readResp), ""
}

func (ss *StorageServer) storageReadHandler(w http.ResponseWriter, r *http.Request) {
	// Decode the request body into PathRequest struct.
	var request ReadFileRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// sanitize the path being read
	if request.Path != "" {
		log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		log.Println("Sanitized to dir ", request.Path)
	}

	str, err := ss.fileRead(fmt.Sprintf("%s/%s", ss.rootPath, request.Path), request.Offset, request.Length)

	if err != "" {

		if request.Path == "" {
			err = "IllegalArgumentException"
		}
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response.
	successResponse := DataReturn{
		Data: str,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK
	json.NewEncoder(w).Encode(successResponse)
}

func makeSizeCallToStorageServer(url string, path string) (bool, int) {
	// Create an instance of the PathRequest struct with the path
	log.Println("Making a size call to storage server for copy", url)
	body := PathRequest{
		Path: path,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Printf("Error marshalling JSON: %s\n", err)
		return false, 0
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Printf("Error creating request: %s\n", err)
		return false, 0
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending request: %s\n", err)
		return false, 0
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s\n", err)
		return false, 0
	}

	//Print the response body
	log.Printf("Response size status: %s\n", resp.Status)
	log.Printf("Response size body: %s\n", responseBody)

	// Convert responseBody to a struct.
	var response SizeReturn
	err = json.Unmarshal(responseBody, &response)
	if err != nil || response.Size == 0 {
		log.Printf("Error unmarshalling response size body: %s\n", err)
		return false, 0
	}

	return true, response.Size
}

func makeReadCallToStorageServer(url string, path string, offset int, length int) (bool, string) {
	// Create an instance of the PathRequest struct with the path
	log.Println("Making a read call to storage server for copy", url)
	body := ReadFileRequest{
		Path:   path,
		Offset: offset,
		Length: length,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Error marshalling JSON: %s\n", err)
		return false, ""
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		fmt.Printf("Error creating request: %s\n", err)
		return false, ""
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %s\n", err)
		return false, ""
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %s\n", err)
		return false, ""
	}

	//Print the response body
	fmt.Printf("Response read status: %s\n", resp.Status)

	// Convert responseBody to a struct.
	var response DataReturn
	err = json.Unmarshal(responseBody, &response)
	if err != nil || response.Data == "" {
		log.Println("Error unmarshalling response read body:", err)
		return false, ""
	}
	log.Println("Response read body:", response)

	return true, response.Data
}

func makeCreateCallToStorageServer(url string, path string) bool {
	// Create an instance of the PathRequest struct with the path
	log.Println("Making a create call to storage server for copy", url)
	body := PathRequest{
		Path: path,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Error marshalling JSON: %s\n", err)
		return false
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		fmt.Printf("Error creating request: %s\n", err)
		return false
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %s\n", err)
		return false
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %s\n", err)
		return false
	}

	//Print the response body
	fmt.Printf("Response create status: %s\n", resp.Status)

	// Convert responseBody to a struct.
	var response BooleanReturn
	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		log.Println("Error unmarshalling response create body:", err)
		return false
	}
	log.Println("Response create body:", response)
	return response.Success
}

func makeDeleteCallToStorageServer(url string, path string) bool {
	// Create an instance of the PathRequest struct with the path
	log.Println("Making a delete call to storage server for copy", url)
	body := PathRequest{
		Path: path,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Error marshalling JSON: %s\n", err)
		return false
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		fmt.Printf("Error creating request: %s\n", err)
		return false
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %s\n", err)
		return false
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %s\n", err)
		return false
	}

	//Print the response body
	fmt.Printf("Response delete status: %s\n", resp.Status)

	// Convert responseBody to a struct.
	var response BooleanReturn
	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		log.Println("Error unmarshalling response delete body:", err)
		return false
	}
	log.Println("Response delete body:", response)
	return response.Success
}

func makeWriteCallToStorageServer(url string, path string, offset int, data string) bool {
	// Create an instance of the PathRequest struct with the path
	log.Println("Making a write call to storage server for copy", url)
	body := WriteFileRequest{
		Path:   path,
		Offset: offset,
		Data:   data,
	}

	// Marshal the requestBody struct into JSON
	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Printf("Error marshalling JSON: %s\n", err)
		return false
	}

	// Prepare a new request with the marshaled JSON body
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Printf("Error creating request: %s\n", err)
		return false
	}

	// Set the Content-Type header to indicate JSON payload
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending request: %s\n", err)
		return false
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %s\n", err)
		return false
	}

	//Print the response body
	log.Printf("Response write status: %s\n", resp.Status)
	log.Printf("Response write body: %s\n", responseBody)

	// Convert responseBody to a struct.
	var response BooleanReturn
	err = json.Unmarshal(responseBody, &response)
	if err != nil || response.Success == false {
		log.Printf("Error unmarshalling write response body: %s\n", err)
		return false
	}

	return true
}

func (ss *StorageServer) copyFile(remotePath string, source string, port int) (bool, string) {
	// check if the path is valid
	if remotePath == "" {
		return false, "IllegalArgumentException"
	}

	// check if the path exists in the source server
	// make a size call
	url := fmt.Sprintf("http://%s:%d/storage_size", source, port)
	exist, size := makeSizeCallToStorageServer(url, remotePath)
	if !exist {
		return false, "FileNotFoundException"
	}

	// try copying it here in the server
	// issue a read
	url = fmt.Sprintf("http://%s:%d/storage_read", source, port)
	success, data := makeReadCallToStorageServer(url, remotePath, 0, size)
	if !success {
		return false, "FileNotFoundException"
	}

	// check if the path exists in the dest server
	// make a size call
	url = fmt.Sprintf("http://%s:%d/storage_size", "localhost", ss.CommandPort)
	exist, _ = makeSizeCallToStorageServer(url, remotePath)

	if !exist {
		// issue a create to the server
		url = fmt.Sprintf("http://%s:%d/storage_create", "localhost", ss.CommandPort)
		success = makeCreateCallToStorageServer(url, remotePath)
	} else {
		// issue a delete to the server
		url = fmt.Sprintf("http://%s:%d/storage_delete", "localhost", ss.CommandPort)
		success = makeDeleteCallToStorageServer(url, remotePath)

		// issue a create to the server
		url = fmt.Sprintf("http://%s:%d/storage_create", "localhost", ss.CommandPort)
		success = makeCreateCallToStorageServer(url, remotePath)
	}

	// issue a write to the server
	url = fmt.Sprintf("http://%s:%d/storage_write", "localhost", ss.CommandPort)
	success = makeWriteCallToStorageServer(url, remotePath, 0, data)
	if !success {
		return false, "FileNotFoundException"
	}

	return true, ""
}

// storageCopyHandler handles the /storage_copy endpoint for copying file from server
func (ss *StorageServer) storageCopyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body into PathRequest struct.
	var request CopyFileRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// sanitize the path being read
	if request.Path != "" {
		log.Println("Sanitizing path of dir ", request.Path)
		request.Path = sanitizePath(request.Path)
		log.Println("Sanitized to dir ", request.Path)
	}

	success, err := ss.copyFile(request.Path, request.ServerIp, request.ServerPort)

	if err != "" {

		if request.Path == "" {
			err = "IllegalArgumentException"
		}
		errorResponse := ExceptionReturn{
			ExceptionType: err,
			ExceptionInfo: err,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound) // 404 Not Found
		json.NewEncoder(w).Encode(errorResponse)
		return
	}

	// Prepare the successful response.
	successResponse := BooleanReturn{
		Success: success,
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

	if len(os.Args) != 5 {
		log.Fatal("Usage: go run storage_server.go <clientPort> <commandPort> <registrationPort> <storagePath>")
	}

	clientPort, _ := strconv.Atoi(os.Args[1])
	commandPort, _ := strconv.Atoi(os.Args[2])
	registrationPort, _ := strconv.Atoi(os.Args[3])
	storagePath := os.Args[4]

	// Now you have the port numbers and storage path
	//fmt.Printf("Client Port: %d\n", clientPort)
	//fmt.Printf("Command Port: %d\n", commandPort)
	////fmt.Printf("Registration Port: %d\n", registrationPort)
	//fmt.Printf("Storage Path: %s\n", storagePath)

	ss := StorageServer{
		namingServerPort: registrationPort,
		CientPort:        clientPort,
		CommandPort:      commandPort,
		files:            listFilesRelativePaths(storagePath),
		rootPath:         storagePath,
	}

	http.HandleFunc("/client-endpoint", ClientHandler)   // Define your client endpoint
	http.HandleFunc("/command-endpoint", CommandHandler) // Define your command endpoint
	http.HandleFunc("/storage_create", ss.storageCreateHandler)
	http.HandleFunc("/storage_size", ss.storageSizeHandler)
	http.HandleFunc("/storage_read", ss.storageReadHandler)
	http.HandleFunc("/storage_write", ss.storageWriteHandler)
	http.HandleFunc("/storage_delete", ss.storageDeleteHandler)
	http.HandleFunc("/storage_copy", ss.storageCopyHandler)

	// register the storage server
	ss.registerStorageServer()

	// Client server
	//wg.Add(1)
	go func() {
		//defer wg.Done()

		log.Println("Client server listening on port", clientPort)
		if err := http.ListenAndServe(":"+strconv.Itoa(clientPort), nil); err != nil {
			log.Fatalf("Client server failed: %v", err)
		}
	}()

	// Command server
	//wg.Add(1)
	go func() {
		//defer wg.Done()
		log.Println("Command server listening on port", commandPort)
		if err := http.ListenAndServe(":"+strconv.Itoa(commandPort), nil); err != nil {
			log.Fatalf("Command server failed: %v", err)
		}
	}()

	select {}
	//wg.Wait() // Wait for all servers to stop
}
