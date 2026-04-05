package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bootdotdev/learn-file-storage-s3-golang-starter/internal/auth"
	"github.com/google/uuid"
)

func (cfg *apiConfig) handlerUploadVideo(w http.ResponseWriter, r *http.Request) {
	// Limit request body to 1GB
	const maxMemory = 1 << 30
	r.Body = http.MaxBytesReader(w, r.Body, maxMemory)

	// Parse and validate video ID from URL path
	videoIDString := r.PathValue("videoID")
	videoID, err := uuid.Parse(videoIDString)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid ID", err)
		return
	}

	// Authenticate user via JWT
	token, err := auth.GetBearerToken(r.Header)
	if err != nil {
		respondWithError(w, http.StatusUnauthorized, "Couldn't find JWT", err)
		return
	}

	userID, err := auth.ValidateJWT(token, cfg.jwtSecret)
	if err != nil {
		respondWithError(w, http.StatusUnauthorized, "Couldn't validate JWT", err)
		return
	}

	// Fetch video metadata and verify ownership
	video, err := cfg.db.GetVideo(videoID)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't find video", err)
		return
	}

	if video.UserID != userID {
		respondWithError(w, http.StatusUnauthorized, "Unauthorized", fmt.Errorf("Unauthorized"))
		return
	}

	fmt.Println("uploading video: ", videoID, "by user: ", userID)
	// Parse multipart form and extract video file
	file, header, err := r.FormFile("video")
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Unable to parse from file", err)
		return
	}
	defer file.Close()

	// Validate Content-Type is video/mp4
	contentType := header.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid Content-Type", err)
		return
	}

	if mediaType != "video/mp4" {
		respondWithError(w, http.StatusBadRequest, "Unsupported media type", err)
		return
	}

	exts, err := mime.ExtensionsByType(mediaType)
	if err != nil || len(exts) == 0 {
		respondWithError(w, http.StatusInternalServerError, "Could not determine file extension", err)
		return
	}

	extension := exts[0]

	// Write uploaded video to a temp file
	videoTempData, err := os.CreateTemp("", "tubely-upload.mp4")
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't create temp file", err)
		return
	}
	defer os.Remove(videoTempData.Name())
	defer videoTempData.Close()

	// Copy uploaded file into temp file
	if _, err = io.Copy(videoTempData, file); err != nil {
		respondWithError(w, http.StatusInternalServerError, "Error saving file", err)
		return
	}

	// Reset read position to beginning for further processing
	if _, err = videoTempData.Seek(0, io.SeekStart); err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't reset file pointer", err)
		return
	}

	// Re-encode video with faststart flag so playback can begin before full download
	fastStartVideoPath, err := processVideoForFastStart(videoTempData.Name())
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't create fast start video", err)
		return
	}
	defer os.Remove(fastStartVideoPath)

	// Open the processed file for S3 upload
	fastStartFile, err := os.Open(fastStartVideoPath)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't open processed video", err)
		return
	}
	defer fastStartFile.Close()

	// Generate a random filename for the S3 object
	key := make([]byte, 32)
	_, err = rand.Read(key)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't generate random key", err)
		return
	}

	// Detect aspect ratio to determine S3 folder prefix
	aspectRatio, err := getVideoAspectRatio(fastStartFile.Name())
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't get aspect ratio", err)
		return
	}

	// Map aspect ratio to orientation prefix for S3 key
	var prefix string
	switch aspectRatio {
	case "16:9":
		prefix = "landscape"
	case "9:16":
		prefix = "portrait"
	default:
		prefix = "other"
	}

	randomName := hex.EncodeToString(key)
	fileName := fmt.Sprintf("%s/%s.%s", prefix, randomName, extension)

	// Upload processed video to S3
	_, err = cfg.s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(cfg.s3Bucket),
		Key:         aws.String(fileName),
		Body:        fastStartFile,
		ContentType: aws.String(mediaType),
	})

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't upload file", err)
		return
	}

	// Build the public S3 URL for the uploaded video
	videoURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s",
		cfg.s3Bucket, cfg.s3Region, fileName)
	video.VideoURL = &videoURL

	// Update video URL in the database
	err = cfg.db.UpdateVideo(video)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Couldn't update video", err)
		return
	}

	respondWithJSON(w, http.StatusOK, video)

}

// processVideoForFastStart re-encodes the video with the moov atom moved to the front,
// allowing streaming playback to start before the full file is downloaded.
// Returns the path to the processed output file.
func processVideoForFastStart(filePath string) (string, error) {

	info, err := os.Stat(filePath)
	if err != nil {
		return "", err
	}
	isFile := info.Mode().IsRegular()
	if !isFile {
		return "", fmt.Errorf("%s is not a File", filePath)
	}

	outputFilePath := fmt.Sprintf("%s.processing", filePath)

	// Run ffmpeg to copy streams and apply faststart flag
	cmd := exec.Command("ffmpeg", "-i", filePath, "-c", "copy", "-movflags", "faststart",
		"-f", "mp4", outputFilePath)
	err = cmd.Run()
	if err != nil {
		return "", err
	}

	return outputFilePath, nil
}

// getVideoAspectRatio uses ffprobe to read the video stream dimensions
// and returns the orientation as "16:9", "9:16", or "other".
func getVideoAspectRatio(filePath string) (string, error) {

	info, err := os.Stat(filePath)
	if err != nil {
		return "", err
	}
	isFile := info.Mode().IsRegular()
	if !isFile {
		return "", fmt.Errorf("%s is not a File", filePath)
	}

	// Use ffprobe to extract stream metadata as JSON
	cmd := exec.Command("ffprobe", "-v", "error", "-print_format", "json", "-show_streams", filePath)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	type Video struct {
		Streams []struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		} `json:"streams"`
	}

	var result Video
	if err = json.Unmarshal(output, &result); err != nil {
		return "", err
	}
	width := result.Streams[0].Width
	height := result.Streams[0].Height

	// Compare ratio with tolerance to handle minor encoding differences
	ratio := float64(width) / float64(height)

	switch {
	case math.Abs(ratio-16.0/9.0) < 0.01:
		return "16:9", nil
	case math.Abs(ratio-9.0/16.0) < 0.01:
		return "9:16", nil
	default:
		return "other", nil
	}
}
