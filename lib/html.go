package lib

import (
	"github.com/gocolly/colly"
)

func GenerateHtmlRecords(config Config) ([]interface{}, error) {
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
	return records, nil
}
