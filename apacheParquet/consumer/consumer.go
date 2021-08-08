package main

import (
	"fmt"
	"io"
	"log"
	"os"

	goparquet "github.com/fraugster/parquet-go"
)

func main() {

	/*for _, file := range flag.Args() {
		if err := printFile(file); err != nil {
			log.Printf("Failed to print file %s: %v", file, err)
		}
	}*/
	if err := printFile("output.parquet"); err != nil {
		fmt.Println(err.Error())
	}
}

func printFile(file string) error {
	r, err := os.Open(file)
	if err != nil {
		return err
	}
	defer r.Close()

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		return err
	}

	log.Printf("Printing file %s", file)
	log.Printf("Schema: %s", fr.GetSchemaDefinition())

	count := 0
	for {
		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading record failed: %w", err)
		}

		log.Printf("Record %d:", count)
		for k, v := range row {
			if vv, ok := v.([]byte); ok {
				v = string(vv)
			}
			log.Printf("\t%s = %v", k, v)
		}

		count++
	}

	log.Printf("End of file %s (%d records)", file, count)
	return nil
}
