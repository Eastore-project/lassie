package db

import (
	"fmt"
	"os"
	"testing"
)

// TestGetCidByPieceCid tests the GetCidByPieceCid function using a hardcoded PieceCID.
func TestGetCidByPieceCid(t *testing.T) {

	// Retrieve MongoDB URI and Database Name from environment variables
	uri := os.Getenv("EASTORE_MONGODB_URI")
	fmt.Println(uri)
	if uri == "" {
		t.Fatal("MONGODB_URI environment variable not set")
	}

	databaseName := os.Getenv("DATABASE_NAME")
	if databaseName == "" {
		t.Fatal("DATABASE_NAME environment variable not set")
	}

	// Initialize the database connection
	err := InitDB(uri)
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}

	// Define the hardcoded PieceCID and expected CID
	const (
		hardcodedPieceCID = "baga6ea4seaqhi5hnv5k3oldioclpiqoucm5hw2hhjrltha4sk37buwxpsstsgpi" // Replace with your actual PieceCID
		expectedCID       = "bafybeib6yvshqvwt6o25n7liro3xztsrm5dfgozbsixa5g5q4ujvf5zezu"      // Replace with the corresponding CID in your DB
	)

	// Call the function under test
	result, err := GetFileByPieceCid(hardcodedPieceCID)
	if err != nil {
		t.Fatalf("GetCidByPieceCid returned error: %v", err)
	}

	// Verify that a CID is returned
	if result == nil {
		t.Fatalf("GetCidByPieceCid returned nil, expected CID: %s", expectedCID)
	}

	// Check if the returned CID matches the expected CID
	if result.CID != expectedCID {
		t.Errorf("GetCidByPieceCid returned CID %s, expected %s", result.CID, expectedCID)
	}

	// Optional: Verify that the document no longer exists if your function deletes it
	// This depends on the implementation details of GetCidByPieceCid
}
