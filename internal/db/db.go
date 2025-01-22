package db

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func InitDB(uri string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    var err error
    client, err = mongo.Connect(ctx, options.Client().ApplyURI(uri))
    if err != nil {
        return err
    }
    return client.Ping(ctx, nil)
}