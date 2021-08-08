package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

/*
date: Timestamp
account: Integer → a account identifier
product: Integer → a product identifier
quantity: Float → how many units of the product is sold to an account
price: Float → what is the total price for the products sold to the account
*/

/*type data struct {
Date     int64
Account  int64
Product  int64
Quantity float64
Price    float64
}*/

const recordNumber = 5
const noOfBatches = 2

func main() {
	f, err := os.OpenFile("output.parquet", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer f.Close()

	schemaDef, _ := parquetschema.ParseSchemaDefinition(
		`message data {
			required int64 date;
			required int64 account;
			required int64 product;
			required double quantity;
			required double price;
		}`)

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	r := rand.New(rand.NewSource(99))
	for nb := 0; nb < noOfBatches; nb++ {
		fmt.Println("Generating data for batch 1")
		for rn := 0; rn < recordNumber; rn++ {
			if err := fw.AddData(map[string]interface{}{
				"date":     time.Now().Unix(),
				"account":  r.Int63n(10000),
				"product":  r.Int63n(10000),
				"quantity": r.Float64() * float64(r.Int63n(10000)),
				"price":    r.Float64()*float64(r.Int63n(10000)) + 1,
			}); err != nil {
				log.Fatalf("Failed to add input to parquet file: %v", err)
			}
		}
		if err = fw.FlushRowGroup(); err != nil {
			fmt.Println(err.Error())
			return
		}

		time.Sleep(5 * time.Second)
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet file writer failed: %v", err)
	}
}
