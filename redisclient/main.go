package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	ctx := context.Background()

	err := client.Set(ctx, "foo", "bar", 0).Err()

	if err != nil {
		panic(err)
	}

	val, err := client.Get(ctx, "foo").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println("foo =", val)
}