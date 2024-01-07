package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type Person struct {
	Age    int
	Gender string
}

type Product struct {
	Name     string
	Brand    string
	Category string
	Price    float64
}
type Transaction struct {
	ID              string
	Person          Person
	SelectedProduct Product
	numbah          int
}

func (p *Person) SetGender() {
	rand.Seed(time.Now().UnixNano())
	gendah := []string{"male", "female"}
	randomIndex := rand.Intn(len(gendah))
	p.Age = 50
	p.Gender = gendah[randomIndex]
}

func setGen() int {
	return 1
}

func (t *Transaction) SetUUID() {
	t.ID = gofakeit.UUID()
}

func GetRandomProduct(products []Product) (Product, error) {
	if len(products) == 0 {
		return Product{}, fmt.Errorf("empty product list")
	}

	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(products))
	return products[randomIndex], nil
}

func main() {
	// Set the random seed for consistent data generation

	// Array of structs
	// products := []Product{
	// 	{Category: "sports", Brand: "Nike", Price: 25, Name: "Nike Shoes"},
	// 	{Category: "sports", Brand: "Adidas", Price: 50, Name: "Adidas Large Cap"},
	// 	{Category: "sports", Brand: "Puma", Price: 75, Name: "Shoes"},
	// 	{Category: "sports", Brand: "Sporta", Price: 25, Name: "Geep bunba"},
	// 	{Category: "sports", Brand: "Geepa", Price: 100, Name: "Gopi bahu"},
	// }
	bobin := &Transaction{}
	bobin.SetUUID()
	bobin.Person.SetGender()

	// Get a random product
	randomProduct, err := GetRandomProduct(products)
	if err != nil {
		fmt.Println(err)
		return
	}
	bobin.SelectedProduct = randomProduct

	fmt.Println(bobin)
}
