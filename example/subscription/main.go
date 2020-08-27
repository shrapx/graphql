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
		}).WithLogger(log.Println)

	defer client.Close()

	/*
		subscription {
			hero {
				id
				name
			}
			character(id: "1003") {
				name
				friends {
					name
					__typename
				}
				appearsIn
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
			return nil
		}

		log.Println("data:", string(*data))
		return nil
	})

	if err != nil {
		panic(err)
	}

	return client.Run()
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
