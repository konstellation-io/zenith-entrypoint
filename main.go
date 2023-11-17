package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/konstellation-io/zenith-entrypoint/internal"
	"github.com/konstellation-io/zenith-entrypoint/internal/compression"
	"github.com/konstellation-io/zenith-entrypoint/internal/config"
	"github.com/konstellation-io/zenith-entrypoint/internal/proto/krepb"
	"github.com/konstellation-io/zenith-entrypoint/internal/proto/publicpb"
	"github.com/konstellation-io/zenith-entrypoint/internal/service"
	"github.com/nats-io/nats.go"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
)

func main() {
	err := config.Init()
	if err != nil {
		log.Fatalf("Error initializing config: %s", err.Error())
	}

	nc, err := nats.Connect(viper.GetString(config.NatsURLKey))
	if err != nil {
		log.Fatalf("Error initializing NATS connection: %s", err.Error())
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Error initilizing JetStream connection: %s", err.Error())
	}

	streamsConfiguration, err := getStreamsConfiguration(js, nc)

	channelsMap := cmap.New[chan *krepb.KreNatsMessage]()

	subscriptions, err := subscribeToSubjects(js, streamsConfiguration, channelsMap)
	if err != nil {
		log.Fatal(err)
	}
	defer unsubscribeAll(subscriptions)

	grpcService := service.NewKreGrpcService(js, nc, streamsConfiguration, channelsMap)
	go runServer(grpcService)

	// Uncomment this to log pending requests that couldn't be processed
	//scheduler := tasks.New()
	//defer scheduler.Stop()
	//
	//_, err = scheduler.Add(&tasks.Task{
	//	Interval: 60 * time.Second,
	//	TaskFunc: func() error {
	//		slog.Info(fmt.Sprintf("They are %d pending requests", channelsMap.Count()))
	//		fmt.Println(channelsMap.Keys())
	//		return nil
	//	},
	//})
	//if err != nil {
	//	log.Fatal(err)
	//}

	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan
}

func unsubscribeAll(subscriptions []*nats.Subscription) {
	for _, s := range subscriptions {
		err := s.Unsubscribe()
		if err != nil {
			slog.Error("Error unsubscribing from subject", "subject", s.Subject, "error", err)
		}
	}
}

func subscribeToSubjects(
	js nats.JetStreamContext,
	streamsConfiguration map[string]*internal.StreamInfo,
	channelsMap cmap.ConcurrentMap[string, chan *krepb.KreNatsMessage],
) ([]*nats.Subscription, error) {
	nodeName := viper.GetString(config.NodeNameKey)
	subscriptions := make([]*nats.Subscription, 0, len(streamsConfiguration))

	for _, streamInfo := range streamsConfiguration {
		consumerName := fmt.Sprintf("%s-%s",
			strings.ReplaceAll(streamInfo.InputSubject, ".", "-"),
			strings.ReplaceAll(
				strings.ReplaceAll(nodeName, ".", "-"),
				" ",
				"-",
			),
		)
		s, err := js.Subscribe(
			streamInfo.InputSubject,
			subscriptionCallback(channelsMap),
			nats.DeliverNew(),
			nats.Durable(consumerName),
			nats.MaxAckPending(-1),
			nats.ManualAck(),
			nats.AckWait(22*time.Hour),
		)
		if err != nil {
			return nil, fmt.Errorf("subscribing to subject %q: %w", streamInfo.InputSubject, err)
		}

		subscriptions = append(subscriptions, s)
	}

	return subscriptions, nil
}

func getStreamsConfiguration(js nats.JetStreamContext, nc *nats.Conn) (map[string]*internal.StreamInfo, error) {
	subjectsFile, err := os.ReadFile(viper.GetString(config.NatsSubjectsFileKey))
	if err != nil {
		log.Fatal(err)
	}

	// Get Subjects to subscribe
	var streamsConfiguration map[string]*internal.StreamInfo

	err = json.Unmarshal(subjectsFile, &streamsConfiguration)
	if err != nil {
		return nil, err
	}

	for _, info := range streamsConfiguration {
		maxSize, err := getMaxMessageSize(js, nc, info.Stream)
		if err != nil {
			return nil, fmt.Errorf("getting max message size for stream %q: %w", info.Stream, err)
		}

		info.MaxMessageSize = maxSize
	}

	return streamsConfiguration, err
}

func runServer(service *service.KreGrpcService) {
	s := grpc.NewServer()

	publicpb.RegisterEntrypointServer(s, service)
	reflection.Register(s)

	listener, err := net.Listen("tcp", "0.0.0.0:9000")
	if err != nil {
		log.Fatal(err)
	}

	if err := s.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

func getMaxMessageSize(js nats.JetStreamContext, nc *nats.Conn, stream string) (int64, error) {
	streamInfo, err := js.StreamInfo(stream)
	if err != nil {
		return 0, fmt.Errorf("error getting stream's max message size: %w", err)
	}

	streamMaxSize := int64(streamInfo.Config.MaxMsgSize)
	serverMaxSize := nc.MaxPayload()

	if streamMaxSize == -1 {
		return serverMaxSize, nil
	}

	if streamMaxSize < serverMaxSize {
		return streamMaxSize, nil
	}

	return serverMaxSize, nil
}

func subscriptionCallback(channelsMap cmap.ConcurrentMap[string, chan *krepb.KreNatsMessage]) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		slog.Info("New message received")

		kreMessage, err := parseToKreNatsMessage(msg.Data)
		if err != nil {
			slog.Error("parsing message to kre nats", "error", err)
			ackMsg(msg)
			return
		}

		resChan, ok := channelsMap.Get(kreMessage.RequestId)
		if !ok {
			slog.Error("Response channel for request not found", "requestID", kreMessage.RequestId)
			ackMsg(msg)
			return
		}

		channelsMap.Remove(kreMessage.RequestId)

		resChan <- kreMessage

		ackMsg(msg)
	}
}

func ackMsg(msg *nats.Msg) {
	ackErr := msg.Ack()
	if ackErr != nil {
		slog.Error("Error ACKing message", "error", ackErr.Error())
	}
}

func parseToKreNatsMessage(rawData []byte) (*krepb.KreNatsMessage, error) {
	var (
		data = rawData
		err  error
	)

	if compression.IsCompressed(rawData) {
		data, err = compression.UncompressData(rawData)
		if err != nil {
			return nil, err
		}
	}

	kreMessage := &krepb.KreNatsMessage{}

	err = proto.Unmarshal(data, kreMessage)
	if err != nil {
		return nil, err
	}

	return kreMessage, nil
}
