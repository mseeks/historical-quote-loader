package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/DannyBen/quandl"
	"github.com/Shopify/sarama"
)

// The formatter for passing messages into Kafka
type message struct {
	Quote string `json:"quote"`
	At    string `json:"at"`
}

func main() {
	startDate, err := time.Parse("2006-01-02", os.Getenv("START_DATE"))
	endDate, err := time.Parse("2006-01-02", os.Getenv("END_DATE"))
	if err != nil {
		fmt.Println(err)
		return
	}

	broker := os.Getenv("KAFKA_ENDPOINT")
	producerTopic := os.Getenv("KAFKA_PRODUCER_TOPIC")

	// init consumer
	brokers := []string{broker}

	quandl.APIKey = os.Getenv("QUANDL_API_KEY")
	equityList := strings.Split(os.Getenv("EQUITY_LIST"), ",")

	shuffle(equityList)

	data := map[string]map[string]string{}

	for _, equity := range equityList {
		fmt.Println("Downloading data for:", equity)
		quotes, err := quandl.GetSymbol(fmt.Sprintf("WIKI/%v", equity), nil)
		if err != nil {
			fmt.Println(err)
		}

		for _, item := range quotes.Data {
			dateString := fmt.Sprintf("%v", item[0])
			closeString := fmt.Sprintf("%v", item[4])
			if data[dateString] == nil {
				data[dateString] = map[string]string{}
			}
			data[dateString][equity] = closeString
		}
	}

	dates := []time.Time{}

	for key := range data {
		time, err := time.Parse("2006-01-02", key)
		if err != nil {
			fmt.Println(err)
		}

		if time.Sub(startDate) >= 0 && time.Sub(endDate) <= 0 {
			dates = append(dates, time)
		}
	}

	sort.Sort(byDate(dates))

	var producer sarama.SyncProducer

	for {
		producer, err = sarama.NewSyncProducer(brokers, nil)
		if err != nil {
			fmt.Println(err)
			continue
		}
		break
	}
	defer producer.Close()

	for _, date := range dates {
		for _, symbol := range equityList {
			quote := data[date.Format("2006-01-02")][symbol]
			if quote == "" {
				continue
			}

			quoteMessage := message{
				Quote: data[date.Format("2006-01-02")][symbol],
				At:    date.UTC().Format("2006-01-02 15:04:05 -0700"),
			}

			jsonMessage, err := json.Marshal(quoteMessage)
			if err != nil {
				fmt.Println(err)
				continue
			}

			producer.SendMessage(&sarama.ProducerMessage{
				Topic: producerTopic,
				Key:   sarama.StringEncoder(symbol),
				Value: sarama.StringEncoder(jsonMessage),
			})
		}
	}
}

// Stub for sorting by date
type byDate []time.Time

func (s byDate) Len() int {
	return len(s)
}
func (s byDate) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byDate) Less(i, j int) bool {
	return s[i].Unix() < s[j].Unix()
}

// Shuffles an array in place
func shuffle(vals []string) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(vals) > 0 {
		n := len(vals)
		randIndex := r.Intn(n)
		vals[n-1], vals[randIndex] = vals[randIndex], vals[n-1]
		vals = vals[:n-1]
	}
}
