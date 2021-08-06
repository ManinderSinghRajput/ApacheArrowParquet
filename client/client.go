package main

import (
	"apacheArrowParquet/go/apis"
	"context"
	"log"

	"google.golang.org/grpc"
)

func main() {

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":9090", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()

	c := apis.NewServiceClient(conn)

	//GetTotalItems
	{
		totalItems, err := c.GetTotalItems(context.Background(), &apis.Empty{})
		if err != nil {
			log.Fatalf("Error when calling GetTotalItems: %s", err)
		}
		log.Printf("Total Items: %d", totalItems.Count)
	}

	//Get No of Unique accounts
	{
		totalItems, err := c.GetUniqueAccountsCount(context.Background(), &apis.Empty{})
		if err != nil {
			log.Fatalf("Error when calling GetUniqueAccountsCount: %s", err)
		}
		log.Printf("Total Unique Accounts: %d", totalItems.Count)
	}

	//Get No of Unique products
	{
		totalItems, err := c.GetUniqueProductsCount(context.Background(), &apis.Empty{})
		if err != nil {
			log.Fatalf("Error when calling GetUniqueProductsCount: %s", err)
		}
		log.Printf("Total Unique Products: %d", totalItems.Count)
	}

	{
		totalQuantity, err := c.GetTotalQuantity(context.Background(), &apis.Empty{})
		if err != nil {
			log.Fatalf("Error when calling GetTotalQuantity: %s", err)
		}
		log.Printf("Total Quantity: %f", totalQuantity.Quantity)
	}

	{
		totalPrice, err := c.GetTotalPrice(context.Background(), &apis.Empty{})
		if err != nil {
			log.Fatalf("Error when calling GetTotalPrice: %s", err)
		}
		log.Printf("Total Price: %f", totalPrice.Price)
	}

}
