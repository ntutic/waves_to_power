package main

import "fmt"

func getPower(location, resolution, yearRange, monthRange string) ([]string, error) {
	dataPath := "data/gribs/"

	hsTp, err := FetchHsTp(dataPath, location, resolution, yearRange, monthRange)
	if err == nil {
		return _, err
	}

	fmt.Println(hsTp)

}

func main() {
	location := "glo"
	resolution := "30m"
	yearRange := "1979:1980"
	monthRange := "1:3"

	if files, err := getPower(location, resolution, yearRange, monthRange); err != nil {
		println("Error getPower: ", err)
	}

	for _, file := range files {
		println(file)
	}
	//err = ReadGrib(file)
	//if err != nil {
	//	panic(err)
	//}
}
