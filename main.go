package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/memory"
	"google.golang.org/grpc"

	"apacheArrowParquet/go/apis"
	"apacheArrowParquet/services"
)

/*
date: Timestamp
account: Integer → a account identifier
product: Integer → a product identifier
quantity: Float → how many units of the product is sold to an account
price: Float → what is the total price for the products sold to the account

*/

func main() {

	//Validate that input file is passed as an argument
	if len(os.Args) < 2 {
		fmt.Println("please pass filename along with binary")
		return
	}

	//Populate data from file
	{
		filename := os.Args[1]

		// Loading csv file
		rFile, err := os.Open(filename) //3 columns
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer rFile.Close()

		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

		schema := arrow.NewSchema(
			[]arrow.Field{
				arrow.Field{Name: "date", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "account", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "product", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "quantity", Type: arrow.PrimitiveTypes.Float64},
				arrow.Field{Name: "price", Type: arrow.PrimitiveTypes.Float64},
			},
			nil, // no metadata
		)

		r := csv.NewReader(
			rFile,
			schema,
			csv.WithAllocator(mem),
			csv.WithChunk(5),
			csv.WithComment('#'),
		)
		defer r.Release()

		r.Retain()
		r.Release()

		fmt.Println("reading records")
		records := make([]array.Record, 0)
		for r.Next() {
			rec := r.Record()
			rec.Retain()
			records = append(records, rec)
		}
		fmt.Println("finished records!")

		//Start Server
		serve(schema, records)

		/*n := 0
		for tr.Next() {
			rec := tr.Record()
			for i, col := range rec.Columns() {
				fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
			}
			n++
		}*/
	}
}

func serve(schema *arrow.Schema, records []array.Record) {
	//Create a grpc services

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := services.Server{
		Schema:  schema,
		Records: records,
	}

	grpcServer := grpc.NewServer()

	apis.RegisterServiceServer(grpcServer, &s)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
