package config

import (
	"github.com/spf13/viper"
)

const (
	RuntimeIDKey        = "RUNTIME_ID"
	WorkflowNameKey     = "WORKFLOW_NAME"
	VersionIDKey        = "VERSION_ID"
	NodeIDKey           = "NODE_ID"
	NodeNameKey         = "NODE_NAME"
	RequestTimeout      = "REQUEST_TIMEOUT"
	NatsURLKey          = "NATS_SERVER"
	NatsSubjectsFileKey = "NATS_SUBJECTS_FILE"
)

func Init() error {
	viper.SetEnvPrefix("KRT")
	viper.AutomaticEnv()

	viper.SetConfigFile("config.yaml")

	return viper.ReadInConfig()
}
