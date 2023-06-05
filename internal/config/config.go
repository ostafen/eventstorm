package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Postgres struct {
	Host     string `mapstructure:"host"`
	User     string `mapstructure:"user"`
	Port     string `mapstructure:"port"`
	DB       string `mapstructure:"db"`
	Password string `mapstructure:"password"`
}

func (p Postgres) ConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", p.Host, p.Port, p.User, p.Password, p.DB)
}

type Engine struct {
	Postgres Postgres `mapstructure:"postgres"`
}

type Log struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type Server struct {
	Port int `mapstructure:"port"`
}

type Config struct {
	Server  `mapstructure:"server"`
	Engine  Engine `mapstructure:"engine"`
	Logging Log    `mapstructure:"logging"`
}

func Read() (*Config, error) {
	viperDefaults()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if len(os.Args) > 1 {
		viper.SetConfigFile(os.Args[1])

		if err := viper.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var c Config
	if err := bindEnv(c); err != nil {
		return nil, err
	}
	if err := viper.Unmarshal(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func viperDefaults() {
	viper.SetDefault("server.port", 2113)
	viper.SetDefault("engine.postgres.port", 5432)
}

func bindEnv(v any) error {
	envKeysMap := map[string]any{}
	if err := mapstructure.Decode(v, &envKeysMap); err != nil {
		return err
	}

	keys := envKeys(envKeysMap)
	for _, key := range keys {
		if err := viper.BindEnv(key); err != nil {
			return err
		}
	}
	return nil
}

func envKeys(m map[string]any) []string {
	keys := make([]string, 0)
	for k, v := range m {
		prefix := k

		if vm, isMap := v.(map[string]any); isMap {
			subkeys := envKeys(vm)
			for _, sk := range subkeys {
				keys = append(keys, prefix+"."+sk)
			}
		} else {
			keys = append(keys, prefix)
		}
	}
	return keys
}
