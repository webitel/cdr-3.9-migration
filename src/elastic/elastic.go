package elastic

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	Ctx     context.Context
	Eclient *elastic.Client
	count   int = 1
)

type Record struct {
	Uuid        string    `json:"uuid,omitempty"`
	Name        string    `json:"name,omitempty"`
	Path        string    `json:"path,omitempty"`
	Domain      string    `json:"domain,omitempty"`
	Private     bool      `json:"private"`
	ContentType string    `json:"content-type,omitempty"`
	Type        int       `json:"type,omitempty"`
	CreatedOn   time.Time `json:"createdOn,omitempty"`
	Size        int       `json:"size,omitempty"`
}

func Connect(host string) {
	var err error
	Ctx = context.Background()
	Eclient, err = elastic.NewClient(elastic.SetURL(host), elastic.SetSniff(false))
	if err != nil {
		log.Println(err.Error())
		return
	}
	_, _, err = Eclient.Ping(host).Do(Ctx)
	if err != nil {
		log.Println(err.Error())
	}
}

func BulkInsert(records []Record) {
	bulkRequest := Eclient.Bulk()
	for _, item := range records {
		var tmpDomain string
		if item.Domain != "" && !strings.ContainsAny(item.Domain, ", & * & \\ & < & | & > & / & ?") {
			tmpDomain = "-" + item.Domain
		}
		req := elastic.NewBulkUpdateRequest().Index(fmt.Sprintf("cdr-a-%v%v", time.Now().UTC().Year(), tmpDomain)).Type("cdr").RetryOnConflict(5).Id(item.Uuid).Upsert(map[string]interface{}{"recordings": make([]bool, 0)}).ScriptedUpsert(true).Script(elastic.NewScriptInline("if(ctx._source.containsKey(\"recordings\")){ctx._source.recordings.add(params.v);}else{ctx._source.recordings = new ArrayList(); ctx._source.recordings.add(params.v);}").Lang("painless").Param("v", item))
		bulkRequest = bulkRequest.Add(req)
	}
	res, err := bulkRequest.Do(Ctx)
	if err != nil {
		log.Printf("ERROR [Elastic]: %s", err)
		return
	}
	if res.Errors {
		for _, item := range res.Items {
			if item["update"].Error != nil {
				log.Printf("ERROR. ID: %s INDEX: %s TYPE: %s REASON: %s", item["update"].Id, item["update"].Index, item["update"].Error.Type, item["update"].Error.Reason)
			}
		}
	} else {
		log.Printf("Elastic: items stored %v, request index: %v", len(records), count)
	}
	count++
}
