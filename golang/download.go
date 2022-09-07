package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func Download(filepath string, url string) error {
	resp, err := http.Get(url)
	fmt.Println(11)
	fmt.Println(resp)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
