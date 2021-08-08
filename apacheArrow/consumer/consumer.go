package main

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"os"
)

func main() {

	rFile, err := os.OpenFile("output.arrow", os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer rFile.Close()

	mem := memory.NewGoAllocator()

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

	/*r.Next()
	fmt.Println(r.Record().NumRows())*/

	n := 0
	for r.Next() {
		rec := r.Record()
		rec.Retain()
		fmt.Println(r.Record().Column(2))
		fmt.Printf("rec[%d]: {", n)
		for i, col := range rec.Columns() {
			fmt.Printf("%q: %v,", rec.ColumnName(i), col)
		}
		rec.Release()
		fmt.Printf("}\n")
		n++
	}
}
