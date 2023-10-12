package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
	"flag"
    "encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/joho/godotenv"
)

var (
	producer AWSKinesis
)

// AWSKinesis struct contain all field needed in kinesis stream
type AWSKinesis struct {
	stream          string
	region          string
	accessKeyID     string
	secretAccessKey string
	partitionKey	string
}

type User struct {
	Id 			string	`json:"id,omitempty"`
	Name		string	`json:"name,omitempty"`
	Age			int		`json:"age,omitempty"`
	Birthdate	time.Time 	`json:"birthdate,omitempty"`
}

// initiate configuration
func init() {
	err := godotenv.Load() //Load .env file
	if err != nil {
		log.Panic(err)
	}
	producer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
}

func main() {
	//load the flags (in case it was been provider)
	strKey := flag.String("k", "key1", "partitionKey")
	flag.Parse()

	// connect to aws-kinesis
	s := session.New(&aws.Config{
		Region:      aws.String(producer.region),
		//Credentials: credentials.NewStaticCredentials(producer.accessKeyID, producer.secretAccessKey,""),
	})

	kc := kinesis.New(s)
	streamName := aws.String(producer.stream)
	_, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Panic(err)
	}

	// prepare data that will be sent. We use data.json file as example data
	//data := openFile()
	data := mockData()
	fmt.Println(data)

	// put data to stream
	fmt.Printf("StreamName: %v\n", producer.stream)

	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(data),
		StreamName:   streamName,
		PartitionKey: aws.String(*strKey),
	})
	if err != nil {
		log.Panic(err)
	}

	fmt.Printf("sucess put record: %v\n", *putOutput)
}

func openFile() string {
	jsonFile, err := os.Open("./msg/data_example1.json")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened data.json")

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	return string(byteValue)
}

func mockData() string {
	user := User{Id: producer.partitionKey, Name:"Eliezer Junior", Age: 40, Birthdate: time.Now()}
	
	fmt.Println(user)
	res, err := json.Marshal(user)
    if err != nil {
        return "err"
    }
	return string(res)
}