package internal

type StreamInfo struct {
	InputSubject   string `json:"input_subject"`
	OutputSubject  string `json:"output_subject"`
	Stream         string `json:"stream"`
	MaxMessageSize int64
}
