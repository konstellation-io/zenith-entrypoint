package grpcservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/konstellation-io/zenith-entrypoint/compression"
	"github.com/konstellation-io/zenith-entrypoint/config"
	"github.com/konstellation-io/zenith-entrypoint/kre"
	"github.com/konstellation-io/zenith-entrypoint/kreproto"
	"github.com/konstellation-io/zenith-entrypoint/zenithproto"
	"github.com/nats-io/nats.go"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type KreGrpcService struct {
	zenithproto.UnimplementedEntrypointServer
	js       nats.JetStreamContext
	nc       *nats.Conn
	subjects map[string]kre.StreamInfo
	//channelsMap *sync.Map
	channelsMap cmap.ConcurrentMap[string, chan *kreproto.KreNatsMessage]
}

// func NewKreGrpcService(js nats.JetStreamContext, nc *nats.Conn, subjects map[string]kre.StreamInfo, channelsMap *sync.Map) *KreGrpcService {
func NewKreGrpcService(js nats.JetStreamContext, nc *nats.Conn, subjects map[string]kre.StreamInfo, channelsMap cmap.ConcurrentMap[string, chan *kreproto.KreNatsMessage]) *KreGrpcService {
	return &KreGrpcService{
		zenithproto.UnimplementedEntrypointServer{},
		js,
		nc,
		subjects,
		channelsMap,
	}
}

func (s *KreGrpcService) UnauthorizedTransactionClassifier(_ context.Context, req *zenithproto.InferenceRequest) (*zenithproto.InferenceResponse, error) {
	requestID := uuid.New().String()

	slog.Info("Processing new message", "requestID", requestID)

	payload, err := anypb.New(req)
	if err != nil {
		slog.Error("Parsing grpc message payload", err)
		return nil, err
	}

	kreRequest := &kreproto.KreNatsMessage{
		RequestId:   requestID,
		Payload:     payload,
		FromNode:    viper.GetString(config.NodeNameKey),
		MessageType: kreproto.MessageType_OK,
	}

	outputMsg, err := proto.Marshal(kreRequest)
	if err != nil {
		slog.Error("Marshaling proto message to bytes", "requestID", requestID, "error", err)
		return nil, err
	}

	streamInfo := s.subjects["UnauthorizedTransactionClassifier"]

	preparedMsg, err := s.prepareOutputMessage(streamInfo.Stream, outputMsg)
	if err != nil {
		slog.Error("Preparing output message", "requestID", requestID, "error", err)
		return nil, err
	}

	responseCh := make(chan *kreproto.KreNatsMessage, 1)

	s.channelsMap.Set(requestID, responseCh)

	_, err = s.js.Publish(streamInfo.OutputSubject, preparedMsg)
	if err != nil {
		slog.Error("Publishing nats message", "requestID", requestID, "error", err)
		return nil, err
	}

	resp := <-responseCh

	var grpcRes zenithproto.InferenceResponse

	err = resp.Payload.UnmarshalTo(&grpcRes)
	if err != nil {
		return nil, err
	}

	return &grpcRes, nil
}

func (s *KreGrpcService) UnauthorizedTransactionFeatureStorage(_ context.Context, _ *zenithproto.DatabaseRequest) (*zenithproto.FeatureStorageResponse, error) {
	return nil, errors.New("unimplemented endpoint")
}

func (s *KreGrpcService) prepareOutputMessage(stream string, msg []byte) ([]byte, error) {
	maxSize, err := s.getMaxMessageSize(stream)
	if err != nil {
		return nil, fmt.Errorf("error getting max message size: %s", err)
	}

	lenMsg := int64(len(msg))
	if lenMsg <= maxSize {
		return msg, nil
	}

	outMsg, err := compression.CompressData(msg)
	if err != nil {
		return nil, err
	}

	lenOutMsg := int64(len(outMsg))
	if lenOutMsg > maxSize {
		slog.Error("compressed message exceeds maximum size allowed: current message size %s, max allowed size %s", sizeInMB(lenOutMsg), sizeInMB(maxSize))
		return nil, errors.New("message to big")
	}

	return outMsg, nil
}

func (s *KreGrpcService) getMaxMessageSize(stream string) (int64, error) {
	streamInfo, err := s.js.StreamInfo(stream)
	if err != nil {
		return 0, fmt.Errorf("error getting stream's max message size: %w", err)
	}

	streamMaxSize := int64(streamInfo.Config.MaxMsgSize)
	serverMaxSize := s.nc.MaxPayload()

	if streamMaxSize == -1 {
		return serverMaxSize, nil
	}

	if streamMaxSize < serverMaxSize {
		return streamMaxSize, nil
	}

	return serverMaxSize, nil
}

func sizeInMB(size int64) string {
	return fmt.Sprintf("%.1f MB", float32(size)/1024/1024)
}
