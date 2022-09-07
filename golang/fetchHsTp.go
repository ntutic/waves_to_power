package main

import (
	"fmt"
	"path/filepath"
)

func FetchHsTp(dataPath, location, resolution, yearRange, monthRange string) ([2][]string, error) {

	var filesHsTp [2][]string

	dates, err := GetDates(yearRange, monthRange)
	for _, date := range dates {
		for i, stat := range [2]string{"hs", "tp"} {
			file := "multi_reanal.glo_" + resolution + "_ext." + stat + "." + date + ".grb2"
			filePath := filepath.Join(dataPath, file)
			filePath, _ = filepath.Abs(filePath)

			matches, err := filepath.Glob(filePath)

			if matches == nil {
				fileUrl := "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/" + date + "/gribs/" + file
				err = Download(filePath, fileUrl)
				if err != nil {
					fmt.Println(25)
					panic(err)
				}
				fmt.Println("Downloaded: " + file)
			} else {
				fmt.Println("     Found: " + file)
			}

			filesHsTp[i] = append(filesHsTp[i], filePath)

		}
	}

	fmt.Println(35)
	fmt.Println(err)
	return filesHsTp, err
}
