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

	"github.com/konstellation-io/zenith-entrypoint/compression"
	"github.com/konstellation-io/zenith-entrypoint/config"
	"github.com/konstellation-io/zenith-entrypoint/grpcservice"
	"github.com/konstellation-io/zenith-entrypoint/kre"
	"github.com/konstellation-io/zenith-entrypoint/kreproto"
	"github.com/konstellation-io/zenith-entrypoint/zenithproto"
	"github.com/madflojo/tasks"
	"github.com/nats-io/nats.go"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
)

// var channelsMap sync.Map
//var channelsMap sync.Map

func subscriptionCallback(channelsMap cmap.ConcurrentMap[string, chan *kreproto.KreNatsMessage]) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		slog.Info("New message received")

		kreMessage, err := parseToKreNatsMessage(msg.Data)
		if err != nil {
			slog.Error("parsing message to kre nats", "error", err)
			ackMsg(msg)
			return
		}

		//resChan, ok := channelsMap.LoadAndDelete(kreMessage.RequestId)
		resChan, ok := channelsMap.Get(kreMessage.RequestId)
		if !ok {
			slog.Error("Response channel for request not found", "requestID", kreMessage.RequestId)
			ackMsg(msg)
			return
		}

		channelsMap.Remove(kreMessage.RequestId)

		//resChan.(chan *kreproto.KreNatsMessage) <- kreMessage
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

func parseToKreNatsMessage(rawData []byte) (*kreproto.KreNatsMessage, error) {
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

	kreMessage := &kreproto.KreNatsMessage{}

	err = proto.Unmarshal(data, kreMessage)
	if err != nil {
		return nil, err
	}

	fmt.Println(kreMessage)

	return kreMessage, nil
}

func main() {
	config.Init()

	nc, err := nats.Connect(viper.GetString(config.NatsURLKey))
	if err != nil {
		log.Fatal(err)
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	subjectsFile, err := os.ReadFile(viper.GetString(config.NatsSubjectsFileKey))
	if err != nil {
		log.Fatal(err)
	}

	// Get Subjects to subscribe
	var subjects map[string]kre.StreamInfo

	err = json.Unmarshal(subjectsFile, &subjects)
	if err != nil {
		log.Fatal(err)
	}

	nodeName := viper.GetString(config.NodeNameKey)
	subscriptions := make([]*nats.Subscription, 0, len(subjects))

	channelsMap := cmap.New[chan *kreproto.KreNatsMessage]()

	for _, streamInfo := range subjects {
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
			nats.ManualAck(),
			nats.AckWait(22*time.Hour),
		)
		if err != nil {
			log.Fatal(err)
		}

		subscriptions = append(subscriptions, s)
	}

	service := grpcservice.NewKreGrpcService(js, nc, subjects, channelsMap)
	go runServer(service)

	scheduler := tasks.New()
	defer scheduler.Stop()

	_, err = scheduler.Add(&tasks.Task{
		Interval: time.Duration(60 * time.Second),
		TaskFunc: func() error {
			slog.Info(fmt.Sprintf("They are %d pending requests", channelsMap.Count()))
			fmt.Println(channelsMap.Keys())
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan

	for _, s := range subscriptions {
		err := s.Unsubscribe()
		if err != nil {
			slog.Error("error unsubscribing")
			log.Fatal(err)
		}
	}
}

func runServer(service *grpcservice.KreGrpcService) {
	s := grpc.NewServer()

	zenithproto.RegisterEntrypointServer(s, service)
	reflection.Register(s)

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", 9000))
	if err != nil {
		log.Fatal(err)
	}

	if err := s.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
