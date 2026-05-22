package config

import (
	"kv/store"
	"log"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Store  store.Config
	Server ServerConfig
}

type ServerConfig struct {
	Addr string
}

func ReadConf(path string) *Config {
	v := viper.New()
	v.SetConfigFile(path)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	if err := v.ReadInConfig(); err != nil {
		log.Panic("解析错误")
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		log.Panic("配置映射错误")
	}
	conf := &cfg
	log.Println("success to read config")

	return conf
}
