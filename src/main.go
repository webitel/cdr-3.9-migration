package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/webitel/cdr-3.9-migration/src/elastic"
	"github.com/webitel/cdr-3.9-migration/src/mongo"
	"github.com/webitel/cdr-3.9-migration/src/rabbit"
)

type Config struct {
	ElasticHost string `json:"elastic_host,omitempty"`
	RabbitHost  string `json:"rabbitmq_host,omitempty"`
	MongoHost   string `json:"mongo_host,omitempty"`
	Cdr         bool   `json:"cdr,omitempty"`
	Recordings  bool   `json:"recordings,omitempty"`
}

func main() {
	config := new(Config)
	err := config.readFromFile()
	if err != nil {
		log.Panicln(err)
	}
	err = config.readFromEnviroment()
	if err != nil {
		log.Panicln(err)
	}
	mongo.Connect(config.MongoHost)
	defer mongo.Session.Close()

	if config.Cdr {
		rabbit.Connect(config.RabbitHost)
		defer rabbit.Connection.Close()
		go mongo.GetFiles()
	}
	if config.Recordings {
		elastic.Connect(config.ElasticHost)
		go mongo.GetRecordings()
	}
	f := make(chan bool, 2)
	<-f
}

func (conf *Config) readFromFile() error {
	filePath := flag.String("c", "./config.json", "Config file path")
	flag.Parse()
	if _, err := os.Stat(*filePath); os.IsNotExist(err) {
		return fmt.Errorf("No found config file: %s", *filePath)
	}
	file, err := ioutil.ReadFile(*filePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(file, conf)
	return err
}
func (conf *Config) readFromEnviroment() error {
	if value := os.Getenv("elastic_host"); value != "" {
		conf.ElasticHost = value
	}
	if value := os.Getenv("rabbitmq_host"); value != "" {
		conf.RabbitHost = value
	}
	if value := os.Getenv("mongo_host"); value != "" {
		conf.MongoHost = value
	}
	if value := os.Getenv("cdr"); value != "" {
		if value == "1" || value == "true" {
			conf.Cdr = true
		} else if value == "0" || value == "false" {
			conf.Cdr = false
		}
	}
	if value := os.Getenv("recordings"); value != "" {
		if value == "1" || value == "true" {
			conf.Recordings = true
		} else if value == "0" || value == "false" {
			conf.Recordings = false
		}
	}
	return nil
}
