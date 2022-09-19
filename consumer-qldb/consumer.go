package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"

	"github.com/amzn/ion-go/ion"
)

var (
	consumer AWSKinesis
)

type Qldb_data struct {
	PayLoad			PayLoad			`ion:"payload"`
	QldbStreamArn	string 			`ion:"qldbStreamArn"`
	RecordType		string 			`ion:"recordType"`
}

type PayLoad struct {
	BlockAddress  	BlockAddress 	`ion:"blockAddress"`
	BlockTimestamp  ion.Timestamp  	`ion:"blockTimestamp"`
	TransactionId	string 			`ion:"transactionId"`
}

 type BlockAddress struct {
	StrandId  	string 	`ion:"strandId"`
	SequenceNo 	int		`ion:"sequenceNo"`
 }

// AWSKinesis struct contain all field needed in kinesis stream
type AWSKinesis struct {
	stream          string
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

// initiate configuration
func init() {
	e := godotenv.Load() //Load .env file
	if e != nil {
		fmt.Print(e)
	}
	consumer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
	fmt.Println(consumer)
}

func main() {
	fmt.Println("Kinesis main")
	// connect to aws-kinesis
	s := session.New(&aws.Config{
		Region:      aws.String(consumer.region),
		Credentials: credentials.NewStaticCredentials(consumer.accessKeyID, consumer.secretAccessKey, ""),
	})
	kc := kinesis.New(s)
	streamName := aws.String(consumer.stream)
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		fmt.Println("Error Stream ..1")
		log.Panic(err)
	}

	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		// ShardIteratorType: aws.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: aws.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		fmt.Println("Error Iterator ..1")
		log.Panic(err)
	}

	shardIterator := iteratorOutput.ShardIterator
	var a *string

	// get data using infinity looping
	// we will attempt to consume data every 1 secons, if no data, nothing will be happen
	fmt.Println("Looping...")
	for {
		// get records use shard iterator for making request
		records, err := kc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})

		// if error, wait until 1 seconds and continue the looping process
		if err != nil {
			fmt.Println("Looping...err -1 : ", err )
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// process the data
		if len(records.Records) > 0 {
			for _, d := range records.Records {

				var decodedResult Qldb_data
				err = ion.Unmarshal(d.Data, &decodedResult) 
				if err != nil {
					fmt.Println("Erro IonUnmarshal : ", err )
					log.Println(err)
					continue
				}
				log.Printf("----------------------------------------------------")
				log.Printf("GetRecords Data: %v\n", decodedResult)

			}
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator || err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}
		shardIterator = records.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}
}