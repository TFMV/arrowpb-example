package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/TFMV/arrowpb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

func main() {
	// Initialize Arrow memory allocator
	mem := memory.NewGoAllocator()

	// Define an Arrow schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	// Create Arrow Record Batch
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Append sample data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	builder.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)

	// Build Arrow Record
	record := builder.NewRecord()
	defer record.Release()

	// Create a RecordReader from the Record
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		log.Fatalf("Failed to recreate RecordReader: %v", err)
	}

	defer reader.Release()

	// Convert Arrow schema to ProtoBuf descriptor
	protoDescriptor := arrowpb.ArrowSchemaToProto(schema)
	fmt.Println("Generated ProtoBuf Schema:\n", protoDescriptor)

	// Convert Arrow batch to ProtoBuf messages
	protoMessages := arrowpb.ArrowBatchToProto(reader, protoDescriptor)

	// Print one ProtoBuf message in binary format
	fmt.Println("\nExample ProtoBuf Message (Binary):", protoMessages[0])

	// Deserialize and print ProtoBuf message
	msgDesc := protoDescriptor.ProtoReflect().Descriptor()
	msg := dynamicpb.NewMessage(msgDesc)
	err = proto.Unmarshal(protoMessages[0], msg)
	if err != nil {
		log.Fatalf("Failed to decode ProtoBuf message: %v", err)
	}
	fmt.Println("\nDecoded ProtoBuf Message:", msg)

	// Convert Arrow batch to JSON and print it
	var jsonBuffer bytes.Buffer
	err = arrowpb.FormatArrowJSON(reader, &jsonBuffer)
	if err != nil {
		log.Fatalf("Failed to format Arrow to JSON: %v", err)
	}
	fmt.Println("\nArrow Record as JSON:\n", jsonBuffer.String())
}
