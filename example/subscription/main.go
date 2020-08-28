// subscription is a test program currently being used for developing graphql package.
// It performs queries against a local test GraphQL server instance.
//
// It's not meant to be a clean or readable example. But it's functional.
// Better, actual examples will be created in the future.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/hgiasac/graphql"
)

func main() {
	flag.Parse()

	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	url := flag.Arg(0)
	client := graphql.NewSubscriptionClient(url).
		WithConnectionParams(map[string]interface{}{
			"headers": map[string]string{
				"x-hasura-admin-secret": "hasura",
			},
		}).WithLog(log.Println).
		OnError(func(sc *graphql.SubscriptionClient, err error) error {
			return err
		})

	defer client.Close()

	/*
		subscription($limit: Int!) {
			users(limit: $limit) {
				id
				name
			}
		}
	*/
	var sub struct {
		User struct {
			ID   graphql.ID
			Name graphql.String
		} `graphql:"users(limit: $limit)"`
	}
	type Int int
	variables := map[string]interface{}{
		"limit": Int(10),
	}
	_, err := client.Subscribe(sub, variables, func(data *json.RawMessage, err error) error {

		if err != nil {
			log.Println("error", err)
			return err
		}

		log.Println("data:", string(*data))
		return nil
	})

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			time.Sleep(5 * time.Second)
			client.Reset()
		}
	}()

	go client.Run()

	time.Sleep(time.Minute)
	return nil
}

// print pretty prints v to stdout. It panics on any error.
func print(v interface{}) {
	w := json.NewEncoder(os.Stdout)
	w.SetIndent("", "\t")
	err := w.Encode(v)
	if err != nil {
		panic(err)
	}
}
