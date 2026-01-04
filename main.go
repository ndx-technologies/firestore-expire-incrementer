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
		redisURL            string
		redisSetKey         string
		firestoreCollection string
		firestoreExpireKey  string
		expireIncrement     time.Duration
	)
	flag.StringVar(&projectID, "project_id", os.Getenv("PROJECT_ID"), "GCP project ID")
	flag.StringVar(&firestoreCollection, "firestore_collection", "", "Firestore collection")
	flag.StringVar(&firestoreExpireKey, "firestore_expire_key", "", "Firestore expire key")
	flag.StringVar(&redisURL, "redis", "", "Redis URL")
	flag.StringVar(&redisSetKey, "redis_set_key", "", "Redis set key")
	flag.DurationVar(&expireIncrement, "expire_increment", 0, "expire increment durtation (e.g. 5m, 24h)")
	flag.Parse()

	if projectID == "" || firestoreCollection == "" || firestoreExpireKey == "" || redisURL == "" || redisSetKey == "" || expireIncrement <= 0 {
		flag.Usage()
		log.Fatal("missing required args")
	}

	ctx := context.Background()

	firestoreClient, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer firestoreClient.Close()

	redisConfig, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatal(err)
	}
	rdb := redis.NewClient(redisConfig)

	ks, err := rdb.SMembers(ctx, redisSetKey).Result()
	if err != nil {
		log.Fatal(err)
	}
	if len(ks) == 0 {
		return
	}

	for _, key := range ks {
		doc, err := firestoreClient.Collection(firestoreCollection).Doc(key).Get(ctx)
		if err != nil {
			if status.Code(err) == grpccodes.NotFound {
				continue
			}
			log.Fatal(err)
		}

		expireAt := time.Now()

		if ts, ok := doc.Data()[firestoreExpireKey].(time.Time); ok && !ts.IsZero() {
			expireAt = ts
		}

		expireAt = expireAt.Add(expireIncrement)

		if _, err := firestoreClient.Collection(firestoreCollection).Doc(key).Set(ctx, map[string]any{firestoreExpireKey: expireAt}, firestore.MergeAll); err != nil {
			log.Fatal(err)
		}
	}

	ksAny := make([]any, len(ks))
	for i, v := range ks {
		ksAny[i] = v
	}

	if err := rdb.SRem(ctx, redisSetKey, ksAny...).Err(); err != nil {
		log.Fatal(err)
	}
}
