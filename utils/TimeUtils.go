package utils

import (
	"strings"
	"time"
)

func DatetimeToUnix(datetime string) int64 {

	if strings.HasPrefix(datetime, "0000") {
		return 0
	}

	loc, _ := time.LoadLocation("Europe/Kiev")
	date, err := time.ParseInLocation("2006-01-02 15:04:05", datetime, loc)

	if err != nil {
		panic(err)
	}

	return date.Unix()
}