package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Structs
type Person struct {
	Age    int    `json:"age"`
	Gender string `json:"gender"`
}

type Product struct {
	Name     string  `json:"name"`
	Vendor   string  `json:"vendor"`
	Category string  `json:"category"`
	Price    float64 `json:"price"`
}

type Transaction struct {
	ID              string  `json:"id"`
	Timestamp       int64   `json:"timestamp"`
	Person          Person  `json:"person"`
	SelectedProduct Product `json:"selectedProduct"`
	PaymentMethod   string  `json:"paymentMethod"`
	NumberOfItems   int     `json:"numberOfItems"`
}

// Connector Config represents the configuration for the connector
type ConnectorConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

func (p *Person) GeneratePersonDetail() {
	rand.Seed(time.Now().UnixNano())
	gender := []string{"male", "female"}
	randomIndex := rand.Intn(len(gender))
	min := 18
	max := 75
	p.Age = rand.Intn(max-min+1) + min
	p.Gender = gender[randomIndex]
}

func (t *Transaction) SetUUID() {
	rand.Seed(time.Now().UnixNano())
	currentTime := time.Now().UTC()
	unixTimestampMillis := currentTime.UnixNano() / int64(time.Millisecond)
	t.ID = gofakeit.UUID()
	t.Timestamp = unixTimestampMillis
	paymentMethod := []string{"credit_card", "gift_card", "voucher", "cash_on_delivery"}
	randomIndex := rand.Intn(len(paymentMethod))
	t.PaymentMethod = paymentMethod[randomIndex]
	min_x := 1
	max_x := 10
	t.NumberOfItems = rand.Intn(max_x-min_x+1) + min_x

}

func GetRandomProduct() Product {
	products := []Product{
		{Name: "Air Zoom Pegasus", Vendor: "Nike", Price: 120.00, Category: "Running Shoes"},
		{Name: "Ultraboost", Vendor: "Adidas", Price: 180.00, Category: "Running Shoes"},
		{Name: "Mercurial Superfly", Vendor: "Nike", Price: 250.00, Category: "Soccer Cleats"},
		{Name: "Predator", Vendor: "Adidas", Price: 220.00, Category: "Soccer Cleats"},
		{Name: "Air Jordan", Vendor: "Nike", Price: 200.00, Category: "Basketball Shoes"},
		{Name: "Harden", Vendor: "Adidas", Price: 160.00, Category: "Basketball Shoes"},
		{Name: "Dri-FIT T-shirt", Vendor: "Nike", Price: 35.00, Category: "Athletic Apparel"},
		{Name: "Tiro Training Pants", Vendor: "Adidas", Price: 45.00, Category: "Athletic Apparel"},
		{Name: "Pro Sports Bra", Vendor: "Nike", Price: 30.00, Category: "Sports Bras"},
		{Name: "Don't Rest Bra", Vendor: "Adidas", Price: 35.00, Category: "Sports Bras"},
		{Name: "Windrunner Jacket", Vendor: "Nike", Price: 90.00, Category: "Track Jackets"},
		{Name: "Essentials 3-stripes Jacket", Vendor: "Adidas", Price: 60.00, Category: "Track Jackets"},
		{Name: "Brasilia Gym Bag", Vendor: "Nike", Price: 50.00, Category: "Gym Bags"},
		{Name: "Defender III Duffel Bag", Vendor: "Adidas", Price: 45.00, Category: "Gym Bags"},
		{Name: "Elite Crew Socks", Vendor: "Nike", Price: 14.00, Category: "Socks"},
		{Name: "Traxion Crew Socks", Vendor: "Adidas", Price: 12.00, Category: "Socks"},
		{Name: "Legacy91 Cap", Vendor: "Nike", Price: 20.00, Category: "Caps and Hats"},
		{Name: "Saturday Cap", Vendor: "Adidas", Price: 22.00, Category: "Caps and Hats"},
		{Name: "SportWatch GPS", Vendor: "Nike", Price: 199.00, Category: "Sports Watches"},
		{Name: "miCoach Smart Run", Vendor: "Adidas", Price: 250.00, Category: "Fitness Trackers"},
		{Name: "Fundamental Yoga Mat", Vendor: "Nike", Price: 35.00, Category: "Yoga Accessories"},
		{Name: "Training Mat", Vendor: "Adidas", Price: 40.00, Category: "Yoga Accessories"},
		{Name: "Hydrastrong Swimsuit", Vendor: "Nike", Price: 50.00, Category: "Swimwear"},
		{Name: "Persistar Goggles", Vendor: "Adidas", Price: 20.00, Category: "Swimwear"},
		{Name: "Flex Stride Shorts", Vendor: "Nike", Price: 50.00, Category: "Sportswear"},
		{Name: "Z.N.E. Hoodie", Vendor: "Adidas", Price: 90.00, Category: "Sportswear"},
		{Name: "Pro Training Tights", Vendor: "Nike", Price: 55.00, Category: "Sportswear"},
		{Name: "Squadra 21 Track Pants", Vendor: "Adidas", Price: 45.00, Category: "Sportswear"},
		{Name: "AeroLayer Running Vest", Vendor: "Nike", Price: 100.00, Category: "Sportswear"},
		{Name: "Alphaskin Sport Tee", Vendor: "Adidas", Price: 35.00, Category: "Sportswear"},
		{Name: "Therma Sphere Gloves", Vendor: "Nike", Price: 25.00, Category: "Accessories"},
		{Name: "Climawarm Beanie", Vendor: "Adidas", Price: 22.00, Category: "Accessories"},
		{Name: "Charge 4", Vendor: "Fitbit", Price: 149.95, Category: "Fitness Trackers"},
		{Name: "Versa 3", Vendor: "Fitbit", Price: 229.95, Category: "Smartwatches"},
		{Name: "Inspire 2", Vendor: "Fitbit", Price: 99.95, Category: "Fitness Trackers"},
		{Name: "Sense", Vendor: "Fitbit", Price: 299.95, Category: "Smartwatches"},
		{Name: "Ace 3", Vendor: "Fitbit", Price: 79.95, Category: "Fitness Trackers for Kids"},
		{Name: "Lux", Vendor: "Fitbit", Price: 149.95, Category: "Fitness Trackers"},
	}

	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(products))
	return products[randomIndex]
}

func generateTransactions(numOfMessages int) {
	// Define Kafka configuration
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
		// "go.produce.channel.size":      10000,
		// "queue.buffering.max.messages": 10000,
		// "queue.buffering.max.kbytes":   4096,
	}

	// Create a new producer
	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	fmt.Println("Kafka running")
	// Define the topic
	topic := "transactions" // Replace with your Kafka topic

	t := &Transaction{}

	// Counters and Timing
	// var successfulMessages int
	startTime := time.Now()
	throughputInterval := 1000 * time.Millisecond
	var messageCount int64 = 0

	go func() {
		for {
			time.Sleep(throughputInterval)
			elapsed := time.Since(startTime).Seconds()
			throughput := float64(messageCount) / elapsed
			fmt.Printf("Throughput: %.2f messages/second\n", throughput)
		}
	}()

	for i := 0; i < numOfMessages; i++ {
		t.SetUUID()
		t.Person.GeneratePersonDetail()
		t.SelectedProduct = GetRandomProduct()

		// Serialize the struct to JSON
		messageBytesT, error_json_t := json.Marshal(t)

		if error_json_t != nil {
			panic(err)
		}

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(messageBytesT),
		}, nil)

		if err != nil {
			fmt.Printf("Failed to produce Transaction: %s\n", err)
		}

		messageCount++
		time.Sleep(time.Microsecond * 10)

		// Wait for message deliveries

		// Calculate throughput

	}

	producer.Flush(15 * 1000)
	producer.Close()
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fmt.Printf("\nTime taken to send %d messages to Kafka: %v\n", numOfMessages, duration)
}

func createConnector(url string, config ConnectorConfig) (map[string]interface{}, error) {
	// Convert the payload to JSON
	jsonPayload, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("error marshalling payload: %v", err)
	}

	// Create a new POST request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")

	// Create an HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read and decode the response
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Check response status code
	if resp.StatusCode != http.StatusCreated {
		return result, fmt.Errorf("%v. Status Code: %d %v", result["message"], resp.StatusCode, err)
	}

	return result, nil
}

// makeGetRequest makes a GET request to the specified URL and returns the response body.
func checkElasticConnectStatus() (string, error) {

	url := "http://kafka-connect:8083/connectors"

	// Make the GET request
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response body: %v", err)
	}

	// Print the response status
	// fmt.Printf("Response Status: %s\n", resp.Status)

	// Parse and print the response body as JSON
	var result interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Pretty-print the JSON response
	prettyJSON, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatalf("Failed to generate pretty JSON: %v", err)
	}

	return string(prettyJSON), nil
}

func uploadFile(url string, headers map[string]string, paramName, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a buffer to write our multipart form data to.
	var b bytes.Buffer
	writer := multipart.NewWriter(&b)

	// Add the file
	part, err := writer.CreateFormFile(paramName, filepath.Base(path))
	if err != nil {
		return err
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return err
	}

	// Close the writer before making the request
	err = writer.Close()
	if err != nil {
		return err
	}

	// Create a new HTTP request with the multipart form data
	req, err := http.NewRequest("POST", url, &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Add additional headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Make the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Print the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println(string(body))

	return nil
}

func main() {

	fmt.Print("****Ecommerce Data Generation Client****\n\n")

	/*
		Create Elasticsearch Connector
	*/
	fmt.Print("Checking [ Kibana ] for liveness...\n")

	kibana_url := "http://kibana:5601"
	req_k, err_k := http.NewRequest("GET", kibana_url, nil)
	if err_k != nil {
		log.Fatalf("[ Kibana ] is not running: %v", err_k)
	} else {
		fmt.Println("[ Kibana ] is running: ", req_k)
	}

	url := "http://kibana:5601/api/saved_objects/_import?createNewCopies=false"
	headers := map[string]string{
		"kbn-xsrf": "true",
	}
	paramName := "file"
	path := "./exports.ndjson"

	err_kibana := uploadFile(url, headers, paramName, path)
	if err_kibana != nil {
		log.Fatalf("Error uploading file in [ Kibana ]: %v", err_kibana)
		os.Exit(1)

	} else {
		fmt.Println("File uploaded successfully to [ Kibana ]")
	}

	/*
		Create Elasticsearch Connector
	*/

	// Define the URL for creating the connector
	url_connector := "http://kafka-connect:8083/connectors"

	// Define the connector configuration
	config := ConnectorConfig{
		Name: "elasticsearch-sink",
		Config: map[string]string{
			"connector.class":                "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			"tasks.max":                      "1",
			"topics":                         "transactions",
			"key.ignore":                     "true",
			"schema.ignore":                  "true",
			"connection.url":                 "http://elastic:9200",
			"type.name":                      "_doc",
			"name":                           "elasticsearch-sink",
			"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "false",
		},
	}

	// Create the connector
	result, err := createConnector(url_connector, config)
	if err != nil {
		fmt.Println("Error creating connector:", err)
	} else {
		fmt.Println("Connector created successfully", result)
	}
	// Check status of Kafka Connector for Elastic Search
	r, e := checkElasticConnectStatus()
	if e != nil {

		log.Fatalf("Error creating the [ connector ]: %v", err)

	} else if r == "[]" {
		fmt.Println("[ Connector ] created but not initialized", r)
	} else {
		fmt.Println("[ Connector ] initialized successfully", r)
	}

	numbPtr := flag.Int("limit", 1000000, "an int")
	flag.Parse()
	fmt.Println("Working with: ", *numbPtr)

	time.Sleep(12 * time.Second)
	generateTransactions(*numbPtr)
	fmt.Println("Generation complete. Ready for restart...")
}
