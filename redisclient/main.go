package redisclient

import "github.com/redis/go-redis/v9"

func NewClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
}