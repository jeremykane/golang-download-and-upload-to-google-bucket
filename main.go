package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

const (
	expirationSec    = 3600000 // URL expiration time in seconds
	bucketName       = "smmf-stag-cloud-storage-01"
	credentialPath   = "/Users/jeremykane/GolandProjects/Test/smmf-20230605-02-7609f3e8659b.json"
	uploadFilePath   = "/Users/jeremykane/GolandProjects/Test/upload.jpeg"
	downloadFilePath = "/Users/jeremykane/GolandProjects/Test/download.jpeg"
)

type Credentials struct {
	PrivateKey  string `json:"private_key"`
	ClientEmail string `json:"client_email"`
	ProjectID   string `json:"project_id"`
}

func main() {
	ctx := context.Background()

	// Read credentials from the JSON file
	creds, err := readCredentials(credentialPath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Read Creds Complete.")

	// Create a storage client with credentials
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credentialPath))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("Create storage client:", client)

	googleAccessID := creds.ClientEmail // Set the correct googleAccessID

	// Generate a pre-signed URL for uploading and downloading
	uuid := uuid.New().String()
	objectName := uuid + ".jpeg"

	uploadURL, err := generateUploadURL(ctx, client, bucketName, objectName, expirationSec, googleAccessID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Upload URL:", uploadURL)

	// Upload a document using the pre-signed URL
	err = uploadDocument(uploadURL, uploadFilePath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Document uploaded successfully!", objectName)

	downloadURL, err := generateDownloadURL(ctx, client, bucketName, objectName, expirationSec)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Download URL:", downloadURL)

	// Download the document using the pre-signed URL
	err = downloadDocument(downloadURL, downloadFilePath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Document downloaded successfully!", objectName)
}

// readCredentials reads the credentials from the JSON file
func readCredentials(credentialsPath string) (Credentials, error) {
	file, err := os.Open(credentialsPath)
	if err != nil {
		return Credentials{}, err
	}
	defer file.Close()

	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return Credentials{}, err
	}

	var credentials Credentials
	err = json.Unmarshal(contents, &credentials)
	if err != nil {
		return Credentials{}, err
	}

	return credentials, nil
}

// generateUploadURL generates a pre-signed URL for uploading a file to Google Cloud Storage.
func generateUploadURL(ctx context.Context, client *storage.Client, bucketName, objectName string, expirationSec int64, googleAccessID string) (string, error) {
	bucket := client.Bucket(bucketName)

	expires := time.Duration(expirationSec) * time.Second
	opts := &storage.SignedURLOptions{
		GoogleAccessID: googleAccessID,
		Method:         http.MethodPut,
		Expires:        time.Now().Add(expires),
	}

	uploadURL, err := bucket.SignedURL(objectName, opts)
	if err != nil {
		return "", err
	}

	return uploadURL, nil
}

// uploadDocument uploads a document using the pre-signed URL.
func uploadDocument(uploadURL, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	req, err := http.NewRequest(http.MethodPut, uploadURL, file)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed with status code %d", resp.StatusCode)
	}

	return nil
}

// generateDownloadURL generates a publicly accessible download URL for a file from Google Cloud Storage.
func generateDownloadURL(ctx context.Context, client *storage.Client, bucketName, objectName string, expirationSec int64) (string, error) {
	bucket := client.Bucket(bucketName)

	expires := time.Duration(expirationSec) * time.Second
	opts := &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(expires),
	}

	downloadURL, err := bucket.SignedURL(objectName, opts)
	if err != nil {
		return "", err
	}

	return downloadURL, nil
}

// downloadDocument downloads a document using the pre-signed URL.
func downloadDocument(downloadURL, filePath string) error {
	resp, err := http.Get(downloadURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileSize, err := io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	if fileSize != resp.ContentLength {
		return fmt.Errorf("downloaded file size does not match original file size")
	}

	return nil
}
