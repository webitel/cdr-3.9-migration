package pg_to_elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/olivere/elastic"
	"strings"
	"time"
)

type PgToElastic struct {
	db *gorm.DB
	el *elastic.Client
}

const LIMIT = 10000

func New(pgConnectionString, elasticHost string) *PgToElastic {
	c := &PgToElastic{}
	var err error

	fmt.Println("Connect to:", pgConnectionString)
	c.db, err = gorm.Open("postgres", pgConnectionString)
	if err != nil {
		panic(err.Error())
	}

	c.el, err = elastic.NewClient(
		elastic.SetURL(elasticHost),
		elastic.SetSniff(false),
		elastic.SetHealthcheckTimeout(time.Second*20),
	)
	if err != nil {
		panic(err.Error())
	}
	ctx := context.Background()
	_, _, err = c.el.Ping(elasticHost).Do(ctx)
	if err != nil {
		panic(err.Error())
	}

	return c
}

func (p *PgToElastic) Start(idA, idB int) {
	//var idA, idB int
	//fmt.Println("Input start leg A Id: ")
	//_, err := fmt.Scanf("%d", &idA)
	//if err != nil {
	//	panic(err.Error())
	//}
	//
	//fmt.Println("Input start leg B Id: ")
	//_, err = fmt.Scanf("%d", &idB)
	//if err != nil {
	//	panic(err.Error())
	//}

	fmt.Printf("Start export to elastic leg A start id: %d\n", idA)
	p.moveLeg("a", idA)
	fmt.Printf("Start export to elastic leg B start id: %d \n", idB)
	p.moveLeg("b", idB)
}

func (p *PgToElastic) moveLeg(legName string, startId int) {
	start := time.Now()
	var id = startId
	var count = 0

	table := fmt.Sprintf("cdr_%s", legName)
	var move = -1
	var operationTime time.Time
	for move != 0 {
		operationTime = time.Now()
		move, id = p.Stream(table, id)
		count += move
		timeTrack(operationTime, fmt.Sprintf("Move leg %s %d[%d] items, last id: %d", legName, move, count, id))
		if move == 0 {
			break
		}
	}
	timeTrack(start, fmt.Sprintf("Move leg %s records %d", legName, count))
}

func (p *PgToElastic) Stream(table string, lastId int) (int, int) {
	rows, err := p.db.Table(table). //Debug().
					Select(`id, event`).
					Where("id > $1", lastId).
					Order("id asc").
					Limit(LIMIT).
					Rows()

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	data := make([]*ElasticCdr, 0, LIMIT)

	for rows.Next() {
		var id int
		var event []byte
		var e *ElasticCdr
		var call map[string]interface{}
		rows.Scan(&id, &event)
		call, err = ToJson(&event)

		if err != nil {
			panic(err.Error())
		}

		e, err = ParseToCdr(call)

		if err != nil {
			panic(err.Error())
		}

		lastId = id
		data = append(data, e)
	}

	if len(data) > 0 {
		if err = p.BulkInsert(data); err != nil {
			panic(err.Error())
		}
	}

	return len(data), lastId
}

func (c *PgToElastic) BulkInsert(data []*ElasticCdr) error {
	bulkRequest := c.el.Bulk()
	for _, item := range data {
		var tmpDomain string
		if item.DomainName != "" && !strings.ContainsAny(item.DomainName, ", & * & \\ & < & | & > & / & ?") {
			tmpDomain = "-" + item.DomainName
		}
		//fmt.Println(strings.ToLower(fmt.Sprintf("%s-%s-%d%s", "cdr", item.Leg, time.Now().UTC().Year(), tmpDomain)))
		req := elastic.NewBulkUpdateRequest().
			Index(strings.ToLower(fmt.Sprintf("%s-%s-%d%s", "cdr", item.Leg, time.Now().UTC().Year(), tmpDomain))).
			Type("cdr").
			RetryOnConflict(2).
			Id(item.Uuid).
			DocAsUpsert(true).
			Doc(item)
		bulkRequest = bulkRequest.Add(req) //.Refresh("false")
	}
	ctx := context.TODO()
	res, err := bulkRequest.Do(ctx)
	if err != nil {
		return err
	}

	if res.Errors {
		fmt.Println(res.Errors)
	}
	return nil
}

func ToJson(event *[]byte) (map[string]interface{}, error) {
	call := map[string]interface{}{}

	if err := json.Unmarshal(*event, &call); err != nil {
		return nil, err
	}
	return call, nil
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}

func (self *PgToElastic) Close() {
	self.db.Close()
}
