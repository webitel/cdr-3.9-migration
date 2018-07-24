package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/webitel/cdr-3.9-migration/src/elastic"
	"github.com/webitel/cdr-3.9-migration/src/mongo"
	"github.com/webitel/cdr-3.9-migration/src/pg_to_elastic"
	"github.com/webitel/cdr-3.9-migration/src/rabbit"
)

type Config struct {
	ElasticHost        string  `json:"elastic_host,omitempty"`
	ElasticBulkCount   int     `json:"elastic_bulk_count,omitempty"`
	RabbitHost         string  `json:"rabbitmq_host,omitempty"`
	MongoHost          string  `json:"mongo_host,omitempty"`
	Cdr                bool    `json:"cdr,omitempty"`
	CdrFilter          *string `json:"cdr_filter,omitempty"`
	Recordings         bool    `json:"recordings,omitempty"`
	PgToElastic        bool    `json:"pg_to_elastic,omitempty"`
	PgConnectionString string  `json:"pg_connection_string"`
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

	if config.PgToElastic {
		p := pg_to_elastic.New(config.PgConnectionString, config.ElasticHost)
		p.Start()
		p.Close()
		return
	}

	mongo.Connect(config.MongoHost)
	defer mongo.Session.Close()

	if config.Cdr {
		rabbit.Connect(config.RabbitHost)
		defer rabbit.Connection.Close()
		mongo.GetFiles(config.CdrFilter)
	}
	if config.Recordings {
		elastic.Connect(config.ElasticHost)
		mongo.GetRecordings(config.ElasticBulkCount)
	}
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
	if value := os.Getenv("elastic_bulk_count"); value != "" {
		i, _ := strconv.Atoi(value)
		conf.ElasticBulkCount = i
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

	if value := os.Getenv("cdr_filter"); value != "" && conf.Cdr {
		conf.CdrFilter = &value
	}

	if value := os.Getenv("recordings"); value != "" {
		if value == "1" || value == "true" {
			conf.Recordings = true
		} else if value == "0" || value == "false" {
			conf.Recordings = false
		}
	}

	if value := os.Getenv("pg_to_elastic"); value != "" {
		if value == "1" || value == "true" {
			conf.PgToElastic = true
		}
	}

	if value := os.Getenv("pg_connection_string"); value != "" {
		conf.PgConnectionString = value
	}

	return nil
}
