package lib

import (
	"strconv"

	"github.com/5amCurfew/xtkt/util"
	"github.com/gocolly/colly"
)

func GenerateWebPageRecords(config util.Config) []interface{} {
	records := make([]interface{}, 0)

	collector := colly.NewCollector(
		colly.AllowedDomains("factretriever.com", "www.factretriever.com"),
	)

	collector.OnHTML(".factsList li", func(element *colly.HTMLElement) {
		factId, _ := strconv.Atoi(element.Attr("id"))
		factDesc := element.Text

		fact := map[string]interface{}{
			"ID":          factId,
			"Description": factDesc,
		}

		records = append(records, fact)
	})

	collector.Visit("https://www.factretriever.com/rhino-facts")

	generateSurrogateKey(records, config)
	return records
}
