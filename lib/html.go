package lib

import (
	"github.com/5amCurfew/xtkt/util"
	"github.com/gocolly/colly"
)

func GenerateHtmlRecords(config util.Config) []interface{} {
	records := make([]interface{}, 0)

	collector := colly.NewCollector()

	collector.OnHTML(*config.Html.ElementsPath, func(e *colly.HTMLElement) {
		record := make(map[string]interface{})

		for _, el := range *config.Html.Elements {
			value := e.ChildText(*el.Path)
			record[*el.Name] = value
		}

		records = append(records, record)
	})

	collector.Visit(*config.URL)

	generateSurrogateKey(records, config)
	return records
}
