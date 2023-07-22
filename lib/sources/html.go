package sources

import (
	lib "github.com/5amCurfew/xtkt/lib"
	"github.com/gocolly/colly"
)

func GenerateHtmlRecords(config lib.Config) ([]interface{}, error) {
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

	return records, nil
}