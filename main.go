package main

import (
	"bytes"
	"context"
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
	// 1) Initialize Arrow memory allocator
	mem := memory.NewGoAllocator()

	// 2) Define an Arrow schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	// 3) Build a Record of sample data
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	builder.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// 4) Create a RecordReader from the single Record
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		log.Fatalf("Failed to create RecordReader: %v", err)
	}
	defer reader.Release()

	// 5) Create a descriptor for the schema
	//    Optionally configure well-known types, wrapper usage, etc. via ConvertConfig.
	cfg := &arrowpb.ConvertConfig{
		UseWellKnownTimestamps: false, // Not using timestamps here
		UseWrapperTypes:        false, // Not using wrappers for this example
		MapDictionariesToEnums: false, // Not using dictionary => enum in this example
	}
	fdp, err := arrowpb.ArrowSchemaToFileDescriptorProto(schema, "example.arrowpb", "MyArrowMessage", cfg)
	if err != nil {
		log.Fatalf("Failed building FileDescriptorProto: %v", err)
	}

	// 6) Compile the descriptor with WKT support
	fd, err := arrowpb.CompileFileDescriptorProto(fdp)
	if err != nil {
		log.Fatalf("Failed to compile descriptor: %v", err)
	}

	// 7) Retrieve the top-level message descriptor
	msgDesc, err := arrowpb.GetTopLevelMessageDescriptor(fd)
	if err != nil {
		log.Fatalf("Failed to get message descriptor: %v", err)
	}

	// 8) Convert the Arrow data -> Protobuf wire-format
	//    We'll do it for all rows in this single RecordReader
	protoMessages, err := arrowpb.ArrowReaderToProtos(context.Background(), reader, msgDesc, cfg)
	if err != nil {
		log.Fatalf("Failed to convert Arrow to protobuf: %v", err)
	}

	// Print one ProtoBuf message in binary (base64-encoded or just raw bytes)
	fmt.Println("Number of Proto messages generated:", len(protoMessages))
	if len(protoMessages) > 0 {
		firstMsg := protoMessages[0]
		fmt.Println("\nExample ProtoBuf Message (Binary bytes):", firstMsg)

		// 9) Deserialize the first Protobuf message
		dynMsg := dynamicpb.NewMessage(msgDesc)
		if err := proto.Unmarshal(firstMsg, dynMsg); err != nil {
			log.Fatalf("Failed to decode Protobuf message: %v", err)
		}
		fmt.Println("Decoded ProtoBuf Message:", dynMsg)
	}

	// 10) Convert Arrow batch to JSON (for debugging) and print it
	//     We need to recreate a fresh RecordReader because the prior one is exhausted.
	jsonReader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		log.Fatalf("Failed to recreate record reader: %v", err)
	}
	defer jsonReader.Release()

	var jsonBuffer bytes.Buffer
	err = arrowpb.FormatArrowJSON(jsonReader, &jsonBuffer)
	if err != nil {
		log.Fatalf("Failed to format Arrow to JSON: %v", err)
	}
	fmt.Println("\nArrow Record as JSON:\n", jsonBuffer.String())
}
