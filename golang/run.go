package main

func main() {
	dataPath := "data/"
	resolution := "30m"
	year := "1979"
	month := "01"
	err := FetchHsTp(dataPath, resolution, year, month)
	if err != nil {
		panic(err)
	}
}
