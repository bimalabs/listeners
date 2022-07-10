package elasticsearch

import (
	"context"
	"strings"
	"time"

	"github.com/goccy/go-json"

	bima "github.com/bimalabs/framework/v4"
	"github.com/bimalabs/framework/v4/events"
	"github.com/bimalabs/framework/v4/models"
	"github.com/olivere/elastic/v7"
)

type CreateSyncElasticsearch struct {
	Service       string
	Elasticsearch *elastic.Client
}

func (c *CreateSyncElasticsearch) Handle(event interface{}) interface{} {
	e := event.(*events.Model)
	if c.Elasticsearch == nil {
		return e
	}

	m := e.Data.(models.GormModel)

	var index strings.Builder

	index.WriteString(c.Service)
	index.WriteString("_")
	index.WriteString(m.TableName())

	result := make(chan error)
	go func(r chan<- error) {
		data, _ := json.Marshal(e.Data)

		_, err := c.Elasticsearch.Index().Index(index.String()).BodyJson(string(data)).Do(context.Background())

		r <- err
	}(result)

	go func(r <-chan error) {
		if <-r == nil {
			m.SetSyncedAt(time.Now())
			e.Repository.Update(m)
		}
	}(result)

	return e
}

func (u *CreateSyncElasticsearch) Listen() string {
	return events.AfterCreateEvent.String()
}

func (c *CreateSyncElasticsearch) Priority() int {
	return bima.HighestPriority + 1
}