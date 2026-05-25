// Package generator produces synthetic e-commerce Transaction events for the
// scale lab. Generators are pure data producers — they emit messages to Kafka
// and update a throughput counter, but they don't manage Kafka Connect, Kibana,
// or any other infrastructure (that's internal/bootstrap).
package generator

import (
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

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

func (p *Person) randomize() {
	gender := []string{"male", "female"}
	p.Gender = gender[rand.Intn(len(gender))]
	p.Age = rand.Intn(75-18+1) + 18
}

func (t *Transaction) randomize() {
	paymentMethods := []string{"credit_card", "gift_card", "voucher", "cash_on_delivery"}
	t.ID = gofakeit.UUID()
	t.Timestamp = time.Now().UTC().UnixMilli()
	t.PaymentMethod = paymentMethods[rand.Intn(len(paymentMethods))]
	t.NumberOfItems = rand.Intn(10-1+1) + 1
}

// NewTransaction returns a fully populated Transaction with randomized fields.
func NewTransaction() Transaction {
	var t Transaction
	t.randomize()
	t.Person.randomize()
	t.SelectedProduct = randomProduct()
	return t
}

func randomProduct() Product {
	return products[rand.Intn(len(products))]
}

var products = []Product{
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
