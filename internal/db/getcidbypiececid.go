package db

import (
	"context"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type File struct {
	ID        string `bson:"id"`
	UserID    string `bson:"user_id"`
	FolderID  string `bson:"folder_id"`
	Size      int    `bson:"size"`
	Name      string `bson:"name"`
	MimeType  string `bson:"mimeType"`
	CID       string `bson:"cid"`
	PieceSize int    `bson:"pieceSize"`
	PieceCID  string `bson:"pieceCid"`
	// Add other fields as needed
}

var client *mongo.Client

// GetFileByPieceCid retrieves a file from the database using its piece CID
func GetFileByPieceCid(pieceCid string) (*File, error) {
	collection := client.Database(os.Getenv("DATABASE_NAME")).Collection("files")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var file File
	filter := bson.M{"pieceCid": pieceCid}
	err := collection.FindOne(ctx, filter).Decode(&file)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	return &file, nil
}
