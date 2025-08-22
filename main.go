package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/redis/go-redis/v9"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	var (
		projectID           string
		redisAddr           string
		redisUser           string
		redisPassword       string
		redisDB             int
		redisSetKey         string
		firestoreCollection string
		firestoreExpireKey  string
		expireIncrement     time.Duration
	)
	flag.StringVar(&projectID, "project-id", os.Getenv("PROJECT_ID"), "GCP project ID")
	flag.StringVar(&firestoreCollection, "firestore-collection", os.Getenv("FIRESTORE_COLLECTION"), "Firestore collection")
	flag.StringVar(&firestoreExpireKey, "firestore-expire-key", os.Getenv("FIRESTORE_EXPIRE_KEY"), "Firestore expire key")
	flag.StringVar(&redisAddr, "redis-addr", os.Getenv("REDIS_ADDR"), "Redis address")
	flag.StringVar(&redisUser, "redis-user", os.Getenv("REDIS_USER"), "Redis user")
	flag.StringVar(&redisPassword, "redis-password", os.Getenv("REDIS_PASSWORD"), "Redis password")
	flag.IntVar(&redisDB, "redis-db", 0, "redis db")
	flag.StringVar(&redisSetKey, "redis-set-key", os.Getenv("REDIS_SET_KEY"), "Redis set key")
	flag.DurationVar(&expireIncrement, "expire-increment", 0, "expire increment")
	flag.Parse()

	ctx := context.Background()

	firestoreClient, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer firestoreClient.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Username: redisUser,
		Password: redisPassword,
		DB:       redisDB,
	})

	keys, err := rdb.SMembers(ctx, redisSetKey).Result()
	if err != nil {
		log.Fatal(err)
	}

	for _, key := range keys {
		doc, err := firestoreClient.Collection(firestoreCollection).Doc(key).Get(ctx)
		if err != nil {
			if status.Code(err) == grpccodes.NotFound {
				continue
			}
			log.Fatal(err)
		}

		expireAt := time.Now()

		if ts, ok := doc.Data()[firestoreExpireKey].(time.Time); ok {
			expireAt = ts
		}

		expireAt = expireAt.Add(expireIncrement)

		if _, err := firestoreClient.Collection(firestoreCollection).Doc(key).Set(ctx, map[string]any{
			firestoreExpireKey: expireAt,
		}, firestore.MergeAll); err != nil {
			log.Fatal(err)
		}
	}

	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		log.Fatal(err)
	}
}
