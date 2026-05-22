package store

import "time"

type Config struct {
	DataFile        string
	HintFile        string
	TmpFile         string
	LRUSize         int
	TTLScanInternal time.Duration
}

var currentConfig Config

func DefaultConfig() Config {
	return Config{
		DataFile:        "nosql.json",
		HintFile:        "nosql.hint",
		TmpFile:         "nosql.tmp",
		LRUSize:         1000,
		TTLScanInternal: 10 * time.Second,
	}
}
