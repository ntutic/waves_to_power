package main

func main() {
	dataPath := "data/gribs/"
	resolution := "30m"
	year := "1979"
	month := "01"

	err := FetchHsTp(dataPath, resolution, year, month)
	if err != nil {
		panic(err)
	}

	file := "data/gribs/multi_reanal.glo_30m_ext.hs.197901.grb2"
	err = ReadGrib(file)
	if err != nil {
		panic(err)
	}
}
