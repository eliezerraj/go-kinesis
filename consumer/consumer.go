package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"
)

var (
	consumer AWSKinesis
)

// AWSKinesis struct contain all field needed in kinesis stream
type AWSKinesis struct {
	stream          string
	region          string
	accessKeyID     string
	secretAccessKey string
}

// initiate configuration
func init() {
	fmt.Println("init")
	err := godotenv.Load() //Load .env file
	if err != nil {
		log.Panic(err)
	}
	consumer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
}

func main() {
	fmt.Println("main")

	s := session.New(&aws.Config{
		Region:      aws.String(consumer.region),
		//Credentials: credentials.NewStaticCredentials(consumer.accessKeyID, consumer.secretAccessKey, ""),
	})

	kc := kinesis.New(s)
	streamName := aws.String(consumer.stream)
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Fatal("kc.DescribeStream ERROR: %v\n", err)
	}

	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		//ShardIteratorType: aws.String("TRIM_HORIZON"),
		//ShardIteratorType: aws.String("AT_SEQUENCE_NUMBER"),
		ShardIteratorType: aws.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		log.Fatal("kc.GetShardIterator ERROR: %v\n", err)
	}

	shardIterator := iteratorOutput.ShardIterator
	var a *string

	for {
		fmt.Printf("read msg from StreamName: %v shardId: %v \n", consumer.stream, *streams.StreamDescription.Shards[0].ShardId)
		// get records use shard iterator for making request
		records, err := kc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		// if error, wait until 1 seconds and continue the looping process
		if err != nil {
			log.Printf("kc.GetRecords ERROR: %v\n", err)
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// process the data
		if len(records.Records) > 0 {
			for _, d := range records.Records {
				m := make(map[string]interface{})
				err := json.Unmarshal([]byte(d.Data), &m)
				if err != nil {
					log.Printf("record iterator ERROR: %v\n", err)
					continue
				}
				log.Printf("GetRecords Data: %v\n", m)
			}
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator || err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}

		shardIterator = records.NextShardIterator
		time.Sleep(2000 * time.Millisecond)
	}
}