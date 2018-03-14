package mongo

import (
	"encoding/json"
	"log"
	"time"

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

	Collection = Session.DB("webitel").C("cdr")
	Recordings = Session.DB("webitel").C("cdrFile")
}

func GetFiles() {
	find := Collection.Find(bson.M{}).Sort("_id")
	items := find.Iter()
	var item interface{}
	var event []byte
	var i int = 1
	for items.Next(&item) {
		event, _ = json.Marshal(item)
		rabbit.Publish(event)
		log.Printf("Rabbit: %v", i)
		i++
	}
	log.Println("Finish cdr")
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
