package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/konstellation-io/zenith-entrypoint/internal"
	"github.com/konstellation-io/zenith-entrypoint/internal/compression"
	"github.com/konstellation-io/zenith-entrypoint/internal/config"
	"github.com/konstellation-io/zenith-entrypoint/internal/proto/krepb"
	"github.com/konstellation-io/zenith-entrypoint/internal/proto/publicpb"
	"github.com/nats-io/nats.go"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var ErrMessageToBig = errors.New("message to big")

type KreGrpcService struct {
	publicpb.UnimplementedEntrypointServer
	js                   nats.JetStreamContext
	nc                   *nats.Conn
	streamsConfiguration map[string]*internal.StreamInfo
	channelsMap          cmap.ConcurrentMap[string, chan *krepb.KreNatsMessage]
}

func NewKreGrpcService(js nats.JetStreamContext, nc *nats.Conn, subjects map[string]*internal.StreamInfo, channelsMap cmap.ConcurrentMap[string, chan *krepb.KreNatsMessage]) *KreGrpcService {
	return &KreGrpcService{
		publicpb.UnimplementedEntrypointServer{},
		js,
		nc,
		subjects,
		channelsMap,
	}
}

func (s *KreGrpcService) UnauthorizedTransactionClassifier(_ context.Context, req *publicpb.InferenceRequest) (*publicpb.InferenceResponse, error) {
	requestID := uuid.New().String()

	slog.Info("Processing new message", "requestID", requestID)

	streamInfo := s.streamsConfiguration["UnauthorizedTransactionClassifier"]

	preparedMsg, err := s.prepareOutputMsg(req, requestID, *streamInfo)
	if err != nil {
		return nil, fmt.Errorf("preparing output message: %w", err)
	}

	responseCh := make(chan *krepb.KreNatsMessage, 1)

	s.channelsMap.Set(requestID, responseCh)

	slog.Info("Publishing nats message", "requestID", requestID)

	_, err = s.js.Publish(streamInfo.OutputSubject, preparedMsg)
	if err != nil {
		slog.Error("Error publishing nats message", "requestID", requestID, "error", err)
		return nil, err
	}

	slog.Debug("Waiting for exitpoint response", "requestID", requestID)

	resp := <-responseCh

	var grpcRes publicpb.InferenceResponse

	err = resp.Payload.UnmarshalTo(&grpcRes)
	if err != nil {
		return nil, err
	}

	slog.Debug("Sending gRPC response", "requestID", requestID)

	return &grpcRes, nil
}

func (s *KreGrpcService) UnauthorizedTransactionFeatureStorage(_ context.Context, _ *publicpb.DatabaseRequest) (*publicpb.FeatureStorageResponse, error) {
	return nil, errors.New("unimplemented endpoint")
}

func (s *KreGrpcService) prepareOutputMsg(
	req *publicpb.InferenceRequest,
	requestID string,
	streamInfo internal.StreamInfo,
) ([]byte, error) {
	outputMsg, err := s.getKreNatsMessageBytes(req, requestID)
	if err != nil {
		return nil, err
	}

	if !s.isCompressionRequired(outputMsg, streamInfo) {
		return outputMsg, err
	}

	outputMsg, err = s.compressMessage(outputMsg, streamInfo)
	if err != nil {
		return nil, fmt.Errorf("compressing message: %w", err)
	}

	return outputMsg, nil
}

func (s *KreGrpcService) getKreNatsMessageBytes(req *publicpb.InferenceRequest, requestID string) ([]byte, error) {
	payload, err := anypb.New(req)
	if err != nil {
		return nil, err
	}

	kreRequest := &krepb.KreNatsMessage{
		RequestId:   requestID,
		Payload:     payload,
		FromNode:    viper.GetString(config.NodeNameKey),
		MessageType: krepb.MessageType_OK,
	}

	outputMsg, err := proto.Marshal(kreRequest)
	if err != nil {
		return nil, err
	}

	return outputMsg, nil
}

func (s *KreGrpcService) isCompressionRequired(msg []byte, streamInfo internal.StreamInfo) bool {
	return int64(len(msg)) >= streamInfo.MaxMessageSize
}

func (s *KreGrpcService) compressMessage(msg []byte, streamInfo internal.StreamInfo) ([]byte, error) {
	outMsg, err := compression.CompressData(msg)
	if err != nil {
		return nil, err
	}

	lenOutMsg := int64(len(outMsg))
	if lenOutMsg > streamInfo.MaxMessageSize {
		slog.Error("compressed message exceeds maximum size allowed: current message size %s, max allowed size %s",
			compression.SizeInMB(lenOutMsg), compression.SizeInMB(streamInfo.MaxMessageSize))
		return nil, ErrMessageToBig
	}

	return outMsg, nil
}
