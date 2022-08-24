package main

import (
	"fmt"
	"path/filepath"
)

func FetchHsTp(dataPath, resolution, year, month string) error {
	yearMonth := year + month
	stats := [2]string{"hs", "tp"}
	var err error
	var matches []string

	for _, stat := range stats {
		file := "multi_reanal.glo_" + resolution + "_ext." + stat + "." + yearMonth + ".grb2"
		filePath := filepath.Join(dataPath, file)
		filePath, _ = filepath.Abs(filePath)

		matches, err = filepath.Glob(filePath)

		if matches == nil {
			fileUrl := "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/" + yearMonth + "/gribs/" + file
			err = Download(filePath, fileUrl)
			if err != nil {
				panic(err)
			}
			fmt.Println("Downloaded: " + file)
		} else {
			fmt.Println("     Found: " + file)
		}

	}

	return err
}
