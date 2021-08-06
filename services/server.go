package services

import (
	"apacheArrowParquet/go/apis"
	"context"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"strconv"
	"strings"
)

type Server struct {
	Schema  *arrow.Schema
	Records []array.Record
}

// GetTotalItems : To get no of invoice lines
func (s Server) GetTotalItems(ctx context.Context, empty *apis.Empty) (*apis.CountAsReply, error) {
	fmt.Println("Called GetTotalItems")

	tr := s.getReader()
	defer tr.Release()

	var count int64
	for tr.Next() {
		count += tr.Record().NumRows()
	}
	return &apis.CountAsReply{
		Count: count,
	}, nil
}

// GetUniqueAccountsCount : Get total no of unique accounts
func (s Server) GetUniqueAccountsCount(ctx context.Context, empty *apis.Empty) (*apis.CountAsReply, error) {
	fmt.Println("Called GetUniqueAccountsCount")

	accMap := make(map[string]bool)

	tr := s.getReader()
	defer tr.Release()

	for tr.Next() {
		it := fmt.Sprintf("%s", tr.Record().Column(1))
		for _, v := range strings.Split(it[1:len(it)-1], " ") {
			accMap[v] = true
		}
	}

	return &apis.CountAsReply{
		Count: int64(len(accMap)),
	}, nil
}

// GetUniqueProductsCount Get total no of unique products
func (s Server) GetUniqueProductsCount(ctx context.Context, empty *apis.Empty) (*apis.CountAsReply, error) {
	fmt.Println("Called GetUniqueProductsCount")

	prodMap := make(map[string]bool)

	tr := s.getReader()
	defer tr.Release()

	for tr.Next() {
		it := fmt.Sprintf("%s", tr.Record().Column(2))
		for _, v := range strings.Split(it[1:len(it)-1], " ") {
			prodMap[v] = true
		}
	}
	return &apis.CountAsReply{
		Count: int64(len(prodMap)),
	}, nil
}

// GetTotalQuantity : Get total count of quantity of different items
func (s Server) GetTotalQuantity(ctx context.Context, empty *apis.Empty) (*apis.GetTotalQuantityReply, error) {
	tr := s.getReader()
	defer tr.Release()

	var quantity float64

	for tr.Next() {
		it := fmt.Sprintf("%s", tr.Record().Column(3))
		for _, v := range strings.Split(it[1:len(it)-1], " ") {
			t, _ := strconv.ParseFloat(v, 64)
			quantity += t
		}
	}

	return &apis.GetTotalQuantityReply{
		Quantity: quantity,
	}, nil
}

// GetTotalPrice : Get total price of different items
func (s Server) GetTotalPrice(ctx context.Context, empty *apis.Empty) (*apis.GetTotalPriceReply, error) {
	tr := s.getReader()
	defer tr.Release()

	var price float64

	for tr.Next() {
		it := fmt.Sprintf("%s", tr.Record().Column(4))
		for _, v := range strings.Split(it[1:len(it)-1], " ") {
			t, _ := strconv.ParseFloat(v, 64)
			price += t
		}
	}

	return &apis.GetTotalPriceReply{
		Price: price,
	}, nil
}

func (s Server) getReader() *array.TableReader {
	table := array.NewTableFromRecords(s.Schema, s.Records)
	defer table.Release()

	table.Retain()
	defer table.Release()

	//Size of chunk can be manipulated as per the requirement
	//Setting it to NumCols just to avoid loop
	tr := array.NewTableReader(table, 100)
	return tr
}
