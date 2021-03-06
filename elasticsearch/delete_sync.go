package elasticsearch

import (
	"context"
	"strings"
	"time"

	bima "github.com/bimalabs/framework/v4"
	"github.com/bimalabs/framework/v4/events"
	"github.com/bimalabs/framework/v4/models"
	"github.com/olivere/elastic/v7"
)

type deleteSync struct {
	service       string
	elasticsearch *elastic.Client
}

func NewDeleteSync(service string, client *elastic.Client) events.Listener {
	return &deleteSync{
		service:       service,
		elasticsearch: client,
	}
}

func (d *deleteSync) Handle(event interface{}) interface{} {
	e := event.(*events.Model)
	if d.elasticsearch == nil {
		return e
	}

	m := e.Data.(models.GormModel)

	var index strings.Builder

	index.WriteString(d.service)
	index.WriteString("_")
	index.WriteString(m.TableName())

	result := make(chan error)
	go func(c chan<- error) {
		query := elastic.NewMatchQuery("Id", e.Id)

		ctx := context.Background()
		result, _ := d.elasticsearch.Search().Index(index.String()).Query(query).Do(ctx)
		if result != nil {
			for _, hit := range result.Hits.Hits {
				d.elasticsearch.Delete().Index(index.String()).Id(hit.Id).Do(ctx)
			}
		}

		c <- nil
	}(result)

	go func(r <-chan error) {
		if <-r == nil {
			m.SetSyncedAt(time.Now())
			e.Repository.Update(m)
		}
	}(result)

	return e
}

func (d *deleteSync) Listen() string {
	return events.AfterDeleteEvent.String()
}

func (d *deleteSync) Priority() int {
	return bima.HighestPriority + 1
}
