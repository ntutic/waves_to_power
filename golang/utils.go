package main

import (
	"fmt"
	"strings"
)

func GetDates(yearRange, monthRange string) ([]string, error) {
	var dates []string

	years := strings.Split(yearRange, ":")
	months := strings.Split(monthRange, ":")

	if len(years) == 1 {
		fmt.Println(years[0])
	}

	if len(months) == 1 {
		fmt.Println(months[0])
	}

	return dates, nil
}
