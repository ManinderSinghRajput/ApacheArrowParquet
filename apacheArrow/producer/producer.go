package main

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"math/rand"
	"os"
	"time"
)

const recordNumber = 2
const noOfBatches = 100

func main() {
	mem := memory.NewGoAllocator()

	wFile, err := os.OpenFile("output.arrow", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

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

	w := ipc.NewWriter(wFile, ipc.WithSchema(schema), ipc.WithAllocator(mem))

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	r := rand.New(rand.NewSource(99))

	for nb := 0; nb < noOfBatches; nb++ {
		fmt.Printf("generating Batch No: %d\n", nb)
		for i := 0; i < recordNumber; i++ {
			b.Field(0).(*array.Int64Builder).Append(time.Now().Unix())
			b.Field(1).(*array.Int64Builder).Append(r.Int63n(10000))
			b.Field(2).(*array.Int64Builder).Append(r.Int63n(10000))
			b.Field(3).(*array.Float64Builder).Append(r.Float64() * float64(r.Int63n(10000)))
			b.Field(4).(*array.Float64Builder).Append(r.Float64()*float64(r.Int63n(10000)) + 1)

			rec := b.NewRecord()
			if err = w.Write(rec); err != nil {
				fmt.Println(err.Error())
			}
			rec.Release()
		}
		time.Sleep(5 * time.Second)
	}
}
