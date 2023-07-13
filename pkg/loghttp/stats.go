package loghttp

// IndexStatsResponse represents the http json response to a stats query
type IndexStatsResponse struct {
	Streams int64 `json:"streams"`
	Chunks  int64 `json:"chunks"`
	Entries int64 `json:"entries"`
	// Bytes   int64 `json:"bytes"`
}
