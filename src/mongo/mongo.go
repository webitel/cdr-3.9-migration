package mongo

import (
	"encoding/json"
	"log"
	"time"

	"fmt"
	"github.com/webitel/cdr-3.9-migration/src/elastic"
	"github.com/webitel/cdr-3.9-migration/src/rabbit"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	Session    *mgo.Session
	Collection *mgo.Collection
	Recordings *mgo.Collection
)

func Connect(host string) {
	var err error
	Session, err = mgo.Dial(host)
	if err != nil {
		panic(err)
	}
	Session.SetMode(mgo.Monotonic, true)
	Session.SetSocketTimeout(time.Minute * 5)

	Collection = Session.DB("webitel").C("cdr")
	Recordings = Session.DB("webitel").C("cdrFile")
}

func getBSONFilter(in *string) bson.M {
	if in == nil {
		return bson.M{}
	}
	var result bson.M
	if err := json.Unmarshal([]byte(*in), &result); err != nil {
		panic(err.Error())
	}
	return result
}

func getStrFromPtr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func GetFiles(filter *string) {
	defer timeTrack(time.Now(), "CDR publish")
	var pipeline []bson.M
	var err error

	if filter != nil {
		pipeline = []bson.M{
			{"$match": getBSONFilter(filter)},
		}
	}

	items := Collection.Pipe(pipeline).AllowDiskUse().Iter()
	var item interface{}
	var event []byte
	var i = 0

	for items.Next(&item) {
		event, err = json.Marshal(item)
		if err != nil {
			fmt.Printf("Error marshal: %s\n", err.Error())
			continue
		}

		rabbit.Publish(event)
		i++

		if i%1000 == 0 {
			fmt.Printf("Publish messages: %v\n", i)
		}
	}

	if err = items.Close(); err != nil {
		panic(err.Error())
	}

	log.Println("CDR publish count message ", i)
}

func GetRecordings(bulk int) {
	find := Recordings.Find(bson.M{}).Sort("_id")
	items := find.Iter()
	var (
		item    bson.M
		records []elastic.Record
	)
	for items.Next(&item) {
		records = append(records,
			elastic.Record{
				Uuid:        getString(item["uuid"]),
				Name:        getString(item["name"]),
				Path:        getString(item["path"]),
				Domain:      getString(item["domain"]),
				Private:     getBoolean(item["private"]),
				ContentType: getString(item["content-type"]),
				Type:        getInteger(item["type"]),
				CreatedOn:   getTime(item["createdOn"]),
				Size:        getInteger(item["size"]),
			})
		if len(records) == bulk {
			elastic.BulkInsert(records)
			records = nil
		}
	}
	if len(records) > 0 {
		elastic.BulkInsert(records)
	}
	log.Println("Finish recordings")
}

func getString(i interface{}) (s string) {
	s, _ = i.(string)
	return
}

func getBoolean(i interface{}) (s bool) {
	s, _ = i.(bool)
	return
}

func getInteger(i interface{}) (s int) {
	s, _ = i.(int)
	return
}

func getTime(i interface{}) (s time.Time) {
	s, _ = i.(time.Time)
	return
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
