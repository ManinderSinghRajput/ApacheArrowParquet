package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"google.golang.org/grpc"

	"apacheArrowParquet/apacheArrow/services"
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

		var records []array.Record
		var recMutex sync.RWMutex
		go func (){
			for {
				// Loading arrow file
				rFile, err := os.OpenFile(filename, os.O_RDONLY, 0644)
				if err != nil {
					fmt.Println(err.Error())
					return
				}

				mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
				var tmpRecords []array.Record
				fmt.Println("reading records")
				r, err := ipc.NewReader(rFile, ipc.WithSchema(schema), ipc.WithAllocator(mem))
				if err != nil {
					fmt.Println(err.Error())
					return
				}

				for r.Next() {
					rec := r.Record()
					rec.Retain()
					tmpRecords = append(tmpRecords, rec)
				}
				r.Release()
				rFile.Close()

				if len(records) != len(tmpRecords){
					recMutex.Lock()
					records = tmpRecords
					recMutex.Unlock()
					fmt.Printf("finished reading %d records!\n", len(records))
				}else{
					fmt.Println("No new records are appended")
				}

				time.Sleep(4 * time.Second)
			}
		}()


		//Start Server
		serve(schema, &records, recMutex)
	}
}

func serve(schema *arrow.Schema, records *[]array.Record, recMutex sync.RWMutex) {
	//Create a grpc services
	fmt.Printf("StartingServing on [%s]\n", ipPort)
	lis, err := net.Listen("tcp", ipPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := services.Server{
		Schema:  schema,
		Records: records,
		RecMutex: recMutex,
	}

	grpcServer := grpc.NewServer()

	apis.RegisterServiceServer(grpcServer, &s)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
