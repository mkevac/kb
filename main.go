package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/dustin/go-humanize"
)

var config struct {
	clientID     string
	groupID      string
	brokers      string
	topic        string
	fetchDefault int64
	fetchMin     int64
	fetchMax     int64
	parallel     int
}

func getBrokerList() []string {
	return strings.Split(config.brokers, ",")
}

func getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_1_0_0
	saramaConfig.ClientID = "meetmaker-kafka-info"
	saramaConfig.Metadata.Full = false
	saramaConfig.Net.DialTimeout = time.Second * 20
	saramaConfig.Net.ReadTimeout = time.Second * 20
	saramaConfig.Net.WriteTimeout = time.Second * 20
	saramaConfig.Consumer.Fetch.Min = int32(config.fetchMin)
	saramaConfig.Consumer.Fetch.Default = int32(config.fetchDefault)
	saramaConfig.Consumer.Fetch.Max = int32(config.fetchMax)
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	return saramaConfig
}

func resetOffsets() (partitions []int32, err error) {
	saramaConfig := getSaramaConfig()

	saramaClient, err := sarama.NewClient(getBrokerList(), saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("NewClient(): %w", err)
	}
	defer func() {
		if err2 := saramaClient.Close(); err2 != nil {
			err = fmt.Errorf("Close(): %w", err)
		}
	}()

	partitions_, err := saramaClient.Partitions(config.topic)
	if err != nil {
		return nil, fmt.Errorf("Partitions(): %w", err)
	}

	saramaOffsetManager, err := sarama.NewOffsetManagerFromClient(config.groupID, saramaClient)
	if err != nil {
		return nil, fmt.Errorf("NewOffsetManagerFromClient(): %w", err)
	}
	defer saramaOffsetManager.Close()

	for _, partition := range partitions {
		pom, err := saramaOffsetManager.ManagePartition(config.topic, partition)
		if err != nil {
			return nil, fmt.Errorf("ManagePartition(): %w", err)
		}
		pom.ResetOffset(sarama.OffsetOldest, "")
	}

	return partitions_, nil
}

type handler struct {
	bytesRead uint64
	msgRead   uint64
}

func (h *handler) Setup(session sarama.ConsumerGroupSession) error {
	for topic, partitions := range session.Claims() {
		log.Printf("received claim for topic %s partitions %v", topic, partitions)
	}
	return nil
}

func (h *handler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		atomic.AddUint64(&h.msgRead, 1)
		atomic.AddUint64(&h.bytesRead, uint64(len(msg.Key))+uint64(len(msg.Value)))
	}
	return nil
}

func (h *handler) printer() {
	var msgReadSaved uint64
	var bytesReadSaved uint64

	for range time.Tick(time.Second) {
		msgRead := atomic.LoadUint64(&h.msgRead)
		bytesRead := atomic.LoadUint64(&h.bytesRead)

		if msgReadSaved == 0 {
			msgReadSaved = msgRead
		}

		if bytesReadSaved == 0 {
			bytesReadSaved = bytesRead
		}

		fmt.Printf("%d msg/sec %s/sec\n", msgRead-msgReadSaved, humanize.Bytes(bytesRead-bytesReadSaved))

		msgReadSaved = msgRead
		bytesReadSaved = bytesRead
	}
}

func startReader(h *handler) {
	saramaConfig := getSaramaConfig()
	//saramaClient, err := sarama.NewClient(getBrokerList(), saramaConfig)
	//if err != nil {
	//	log.Fatalf("NewClient(): %s", err)
	//}

	saramaConsumerGroup, err := sarama.NewConsumerGroup(getBrokerList(), config.groupID, saramaConfig)
	if err != nil {
		log.Fatalf("NewConsumerGroup(): %s", err)
	}
	defer saramaConsumerGroup.Close()

	if err := saramaConsumerGroup.Consume(context.Background(), []string{config.topic}, h); err != nil {
		log.Fatalf("Consume(): %s", err)
	}
}

func main() {

	flag.StringVar(&config.clientID, "clientid", "kb-client", "clientID")
	flag.StringVar(&config.groupID, "groupid", "kb-group", "groupID")
	flag.StringVar(&config.brokers, "brokers", "localhost:9092", "broker list")
	flag.StringVar(&config.topic, "topic", "", "topic name")
	flag.IntVar(&config.parallel, "parallel", 5, "parallel")
	flag.Int64Var(&config.fetchDefault, "fetchdefault", 1024*1024, "fetch default bytes")
	flag.Int64Var(&config.fetchMin, "fetchmin", 1, "fetch min bytes")
	flag.Int64Var(&config.fetchMax, "fetchmax", 0, "fetch max bytes")
	flag.Parse()

	partitions, err := resetOffsets()
	if err != nil {
		log.Fatalf("resetOffsets(): %s", err)
	}

	log.Printf("partitions %v", partitions)

	h := handler{}
	go h.printer()

	for i := 0; i < config.parallel; i++ {
		go startReader(&h)
	}

	select {}
}
