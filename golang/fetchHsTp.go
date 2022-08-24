package main

import (
	"fmt"
)

func FetchHsTp(dataPath, resolution, year, month string) error {
	yearMonth := year + month
	stats := [2]string{"hs", "tp"}
	var err error

	for _, stat := range stats {
		file := "multi_reanal.glo_" + resolution + "_ext." + stat + "." + yearMonth + ".grb2"
		fileUrl := "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/" + yearMonth + "/gribs/" + file
		err = Download(dataPath+file, fileUrl)
		if err != nil {
			panic(err)
		}
		fmt.Println("Downloaded: " + file)
	}

	return err
}
