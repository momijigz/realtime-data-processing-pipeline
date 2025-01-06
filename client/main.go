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

	"github.com/briandowns/spinner"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// var Red = "\033[31m"
// var Green = "\033[32m"
// var Reset = "\033[0m"

// var Good = string(Green + "✔ " + Reset)
// var Bad = Red + "✖ " + Reset

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
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Person    Person `json:"person"`
	// SelectedProduct productsmodule.Product `json:"selectedProduct"`
	SelectedProduct Product `json:"selectedProduct"`
	PaymentMethod   string  `json:"paymentMethod"`
}

// ConnectorConfig represents the configuration for the connector
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
		// "bootstrap.servers": "localhost:29092",

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
	// var maxThroughput float32 = 0

	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Build our new spinner
	s.Suffix = " "
	s.Color("red")
	s.Start() // Start the spinner

	go func() {
		for {
			time.Sleep(throughputInterval)
			elapsed := time.Since(startTime).Seconds()
			throughput := float64(messageCount) / elapsed
			fmt.Printf("Throughput: %.2f messages/second\n", throughput)
		}
	}()
	// finishChan <- startMsg{}

	for i := 0; i < numOfMessages; i++ {
		t.SetUUID()
		t.Person.GeneratePersonDetail()
		t.SelectedProduct = GetRandomProduct()

		// Serialize the struct to JSON
		messageBytes, errorora := json.Marshal(t)

		if errorora != nil {
			panic(err)
		}

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(messageBytes),
		}, nil)

		if err != nil {
			fmt.Printf("Failed to produce message: %s\n", err)
		}
		// fmt.Println(t)
		messageCount++
		time.Sleep(time.Microsecond * 1)
		// Wait for message deliveries

		// Calculate throughput

	}
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	s.Stop()
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

	fmt.Print("****ECC Data Generation Client****\n\n")
	// fmt.Println("Checking to see if the required containers are running...")

	/*
		Create Elasticsearch Connector
	*/
	fmt.Print("Checking kibana...\n")

	kibana_url := "http://kibana:5601"
	req_k, err_k := http.NewRequest("GET", kibana_url, nil)
	if err_k != nil {
		log.Fatalf("Kibana is not running: %v", err_k)
	} else {
		fmt.Println("Kibana is running: ", req_k)
	}

	url := "http://kibana:5601/api/saved_objects/_import?createNewCopies=false"
	headers := map[string]string{
		"kbn-xsrf": "true",
	}
	paramName := "file"
	path := "./exports.ndjson"

	err2 := uploadFile(url, headers, paramName, path)
	if err2 != nil {
		log.Fatalf("Error uploading file in Kibana: %v", err2)
		os.Exit(1)

		// fmt.Println("Error uploading file:", err2)
	} else {
		fmt.Println("File uploaded successfully to Kibana")
	}

	/*
		Create Elasticsearch Connector
	*/

	// Define the URL
	//! ORIGINAL
	// urlz := "http://elastic:8083/connectors"

	urlz := "http://kafka-connect:8083/connectors"

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
	result, err := createConnector(urlz, config)
	if err != nil {

		// log.Fatalf("Error creating connector: %v", err)

		fmt.Println("Error creating connector:", err)
	} else {
		fmt.Println("Connector created successfully", result)
	}
	// Check status of Kafka Connector for Elastic
	r, e := checkElasticConnectStatus()
	if e != nil {

		log.Fatalf("Error creating connector: %v", err)

		// fmt.Println("Error creating connector:", e)
	} else if r == "[]" {
		fmt.Println("Connector created but not initialized", r)
	} else {
		fmt.Println("Connector initialized successfully", r)
	}

	numbPtr := flag.Int("limit", 1000000, "an int")
	flag.Parse()
	fmt.Println("Working with: ", *numbPtr)

	time.Sleep(10 * time.Second)
	generateTransactions(*numbPtr)
	// ORIGIANL
	// url := "http://localhost:5601/api/saved_objects/_import?createNewCopies=false"

}
