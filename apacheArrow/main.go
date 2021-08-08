package main

import (
	"apacheArrowParquet/services"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"google.golang.org/grpc"

	"apacheArrowParquet/go/apis"
)

/*
date: Timestamp
account: Integer → a account identifier
product: Integer → a product identifier
quantity: Float → how many units of the product is sold to an account
price: Float → what is the total price for the products sold to the account
*/

var ipPort = ":9090"

func main() {

	//Validate that input file is passed as an argument
	if len(os.Args) < 2 {
		fmt.Println("please pass filename along with executable")
		return
	}

	//Populate data from file
	{
		filename := os.Args[1]

		// Loading arrow file
		rFile, err := os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Println(err.Error())
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

		r, err := ipc.NewReader(rFile, ipc.WithSchema(schema), ipc.WithAllocator(mem))
		if err != nil {
			fmt.Println(err.Error())
			return
		}
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
		fmt.Printf("finished reading %d records!\n", len(records))

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
	fmt.Printf("StartingServing on [%s]\n", ipPort)
	lis, err := net.Listen("tcp", ipPort)
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
