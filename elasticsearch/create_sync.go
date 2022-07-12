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

type createSync struct {
	service       string
	elasticsearch *elastic.Client
}

func NewCreateSync(service string, client *elastic.Client) events.Listener {
	return &createSync{
		service:       service,
		elasticsearch: client,
	}
}

func (c *createSync) Handle(event interface{}) interface{} {
	e := event.(*events.Model)
	if c.elasticsearch == nil {
		return e
	}

	m := e.Data.(models.GormModel)

	var index strings.Builder

	index.WriteString(c.service)
	index.WriteString("_")
	index.WriteString(m.TableName())

	result := make(chan error)
	go func(r chan<- error) {
		data, _ := json.Marshal(e.Data)

		_, err := c.elasticsearch.Index().Index(index.String()).BodyJson(string(data)).Do(context.Background())

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

func (u *createSync) Listen() string {
	return events.AfterCreateEvent.String()
}

func (c *createSync) Priority() int {
	return bima.HighestPriority + 1
}
