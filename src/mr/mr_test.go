package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestJson(t *testing.T) {
	filename := "/root/65840/src/main/mr-1-1"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	kva := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	fmt.Printf("kva: %v\n", kva)
}
