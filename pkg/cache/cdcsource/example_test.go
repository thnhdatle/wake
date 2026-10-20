// SPDX-License-Identifier: Apache-2.0

package cdcsource_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thnhdatle/wake/pkg/cache"
	"github.com/thnhdatle/wake/pkg/cache/cdcsource"
	"github.com/thnhdatle/wake/pkg/event"
)

func ExampleParseTemplate() {
	t, err := cdcsource.ParseTemplate("user:{tenant}:{{{id}}}")
	if err != nil {
		panic(err)
	}
	fmt.Println(t.Columns())

	_, err = cdcsource.ParseTemplate("user:{id")
	var te *cdcsource.TemplateError
	fmt.Println(errors.As(err, &te), te.Offset, te.Reason)
	// Output:
	// [tenant id]
	// true 5 unclosed placeholder
}

func ExampleNewMapping() {
	m, err := cdcsource.NewMapping(map[string][]string{
		"public.users":  {"user:{id}", "user:email:{email}"},
		"public.orders": {"order:{id}"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(m.Tables())
	fmt.Println(m.Templates("public.users")[1])
	// Output:
	// [public.orders public.users]
	// user:email:{email}
}

func ExampleMapping_Keys() {
	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}", "user:email:{email}"}})
	if err != nil {
		panic(err)
	}
	// An update that changed the e-mail address (REPLICA IDENTITY FULL).
	e, err := event.Decode([]byte(`{"id":"0/16B3800-0","source":{"db":"app","schema":"public","table":"users"},
		"op":"u","ts_ms":1760600001000,"lsn":"0/16B3800","tx_id":7813,"key":{"id":9223372036854775807},
		"before":{"id":9223372036854775807,"email":"a@example.com"},
		"after":{"id":9223372036854775807,"email":"b@example.com"},"schema_version":3}`))
	if err != nil {
		panic(err)
	}
	for _, k := range m.Keys(e) {
		fmt.Println(k)
	}
	// Output:
	// user:9223372036854775807
	// user:email:a@example.com
	// user:email:b@example.com
}

func ExampleMapping_CheckReplicaIdentity() {
	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}", "user:email:{email}"}})
	if err != nil {
		panic(err)
	}
	err = m.CheckReplicaIdentity([]cdcsource.TableInfo{{
		Schema: "public", Table: "users",
		Columns: []string{"id", "email"}, IdentityColumns: []string{"id"}, // default identity
	}})
	fmt.Println(errors.Is(err, cdcsource.ErrRequiresFullIdentity))
	// Output:
	// true
}

// sliceSource delivers one batch, then waits for the end of ctx.
type sliceSource [][]byte

func (s sliceSource) Consume(ctx context.Context, handle cdcsource.Handler) error {
	if err := handle(ctx, s); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func ExampleNew() {
	users, err := cache.New[string]("users", cache.JSONCodec[string]()) // add cache.WithL2 in production
	if err != nil {
		panic(err)
	}
	defer users.Close()
	ctx := context.Background()
	_ = users.Set(ctx, "user:42", "Ada")

	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}"}})
	if err != nil {
		panic(err)
	}
	src := sliceSource{[]byte(`{"id":"0/16B3B00-0","source":{"db":"app","schema":"public","table":"users"},` +
		`"op":"d","ts_ms":1760600004000,"lsn":"0/16B3B00","tx_id":7816,"key":{"id":42},"before":{"id":42},` +
		`"after":null,"schema_version":3}`)}
	inv, err := cdcsource.New(src, m, users)
	if err != nil {
		panic(err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := inv.Run(runCtx); err != nil {
		panic(err)
	}
	_, ok, _ := users.Get(ctx, "user:42")
	fmt.Println("cached:", ok)
	// Output:
	// cached: false
}

func ExampleNewKafkaSource() {
	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}"}})
	if err != nil {
		panic(err)
	}
	src, err := cdcsource.NewKafkaSource(cdcsource.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		TopicPrefix: "cdc",
		Group:       "wake-cache-invalidator",
		SASL:        cdcsource.SASL{Mechanism: cdcsource.SASLScramSHA512, User: "wake", Password: "secret"},
	}, m)
	if err != nil {
		panic(err)
	}
	fmt.Println(src.Topics())
	// Output:
	// [cdc.public.users]
}

func ExampleNewNATSSource() {
	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}"}})
	if err != nil {
		panic(err)
	}
	src, err := cdcsource.NewNATSSource(cdcsource.NATSConfig{
		URL:           "nats://localhost:4222",
		SubjectPrefix: "cdc",
		Stream:        "wake_app",
		Durable:       "wake-cache-invalidator",
	}, m)
	if err != nil {
		panic(err)
	}
	fmt.Println(src.Subjects())
	// Output:
	// [cdc.public.users]
}
