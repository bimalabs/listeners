package elasticsearch

import (
	"strings"

	"github.com/bimalabs/framework/v4/events"
	"github.com/olivere/elastic/v7"
)

type paginationFilter struct {
}

func NewPaginationFilter() events.Listener {
	return &paginationFilter{}
}

func (p *paginationFilter) Handle(event interface{}) interface{} {
	e, ok := event.(*events.ElasticsearchPagination)
	if !ok {
		return event
	}

	var wildCard strings.Builder
	for _, v := range e.Filters {
		wildCard.Reset()
		wildCard.WriteString("*")
		wildCard.WriteString(v.Value)
		wildCard.WriteString("*")

		q := elastic.NewWildcardQuery(v.Field, wildCard.String())
		q.Boost(1.0)
		e.Query.Must(q)
	}

	return e
}

func (p *paginationFilter) Listen() string {
	return events.PaginationEvent.String()
}

func (p *paginationFilter) Priority() int {
	return 255
}
