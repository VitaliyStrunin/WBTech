package config

import (
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type databaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

type kafkaConfig struct {
	Brokers []string
	Topic   string
}

type Config struct {
	databaseConfig
	kafkaConfig
}

func NewConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Printf("Переменные окружения не загружены: %v, используются системные переменные", err)
	}
	return &Config{
		databaseConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnv("DB_PORT", "5432"),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Database: getEnv("DB_DATABASE", "wb_database"),
		},
		kafkaConfig{
			Brokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
			Topic:   getEnv("KAFKA_TOPIC", "orders"),
		},
	}
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}
