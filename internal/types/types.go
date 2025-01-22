package types

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