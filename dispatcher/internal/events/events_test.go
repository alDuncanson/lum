package events

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sync"
	"testing"
	"time"
)

func TestSubscribeReplaysRingBufferOldestFirst(t *testing.T) {
	bus := NewBus(2)
	bus.Publish(Event{Kind: KindScanStarted, SourceID: "a"})
	bus.Publish(Event{Kind: KindScanStarted, SourceID: "b"})
	bus.Publish(Event{Kind: KindScanStarted, SourceID: "c"}) // ring cap 2: "a" evicted

	_, backlog, unsubscribe := bus.Subscribe(4)
	defer unsubscribe()
	if len(backlog) != 2 {
		t.Fatalf("backlog length = %d, want 2", len(backlog))
	}
	if backlog[0].SourceID != "b" || backlog[1].SourceID != "c" {
		t.Fatalf("backlog = %+v, want [b, c] oldest first", backlog)
	}
	if backlog[0].Seq >= backlog[1].Seq {
		t.Fatalf("backlog not in ascending seq order: %+v", backlog)
	}
}

func TestPublishFansOutToLiveSubscribersOnly(t *testing.T) {
	bus := NewBus(0)
	ch, backlog, unsubscribe := bus.Subscribe(4)
	if len(backlog) != 0 {
		t.Fatalf("backlog on empty bus = %+v, want none", backlog)
	}

	bus.Publish(Event{Kind: KindDocumentIngested, DocumentID: "doc1"})
	select {
	case e := <-ch:
		if e.DocumentID != "doc1" || e.Seq != 1 {
			t.Fatalf("got %+v, want doc1 seq 1", e)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive published event")
	}

	unsubscribe()
	bus.Publish(Event{Kind: KindDocumentIngested, DocumentID: "doc2"})
	select {
	case e, ok := <-ch:
		if ok {
			t.Fatalf("unsubscribed channel received %+v", e)
		}
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPublishNeverBlocksOnSlowSubscriber(t *testing.T) {
	bus := NewBus(0)
	_, _, unsubscribe := bus.Subscribe(1) // capacity 1, nobody reads
	defer unsubscribe()

	done := make(chan struct{})
	go func() {
		for range 10 {
			bus.Publish(Event{Kind: KindSnapshot})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber channel")
	}
}

func TestSequenceNumbersAreMonotonicUnderConcurrentPublish(t *testing.T) {
	bus := NewBus(0)
	ch, _, unsubscribe := bus.Subscribe(200)
	defer unsubscribe()

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				bus.Publish(Event{Kind: KindSnapshot})
			}
		}()
	}
	wg.Wait()

	seen := make(map[uint64]bool)
	for range 100 {
		e := <-ch
		if seen[e.Seq] {
			t.Fatalf("duplicate sequence number %d", e.Seq)
		}
		seen[e.Seq] = true
	}
}

func TestPublishStampsTimeWhenUnset(t *testing.T) {
	bus := NewBus(1)
	before := time.Now()
	bus.Publish(Event{Kind: KindSnapshot})
	_, backlog, unsubscribe := bus.Subscribe(1)
	defer unsubscribe()
	if len(backlog) != 1 {
		t.Fatalf("backlog = %+v, want 1 event", backlog)
	}
	if backlog[0].Time.Before(before) {
		t.Fatalf("event time %v predates publish call %v", backlog[0].Time, before)
	}
}

// AllKinds is hand-maintained, and a kind missing from it is invisible to
// `lum events --kinds` and to anyone discovering the `?types=` filter. Parse
// the declarations and compare, so adding a constant without listing it
// fails here rather than silently shipping a gap.
func TestAllKindsIsComplete(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "events.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	declared := make(map[string]bool)
	for _, decl := range parsed.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			ident, ok := value.Type.(*ast.Ident)
			if !ok || ident.Name != "Kind" {
				continue
			}
			for _, name := range value.Names {
				declared[name.Name] = true
			}
		}
	}
	if len(declared) == 0 {
		t.Fatal("found no Kind constants; this test is not checking anything")
	}

	listed := make(map[Kind]bool, len(AllKinds))
	for _, kind := range AllKinds {
		listed[kind] = true
	}
	if len(listed) != len(AllKinds) {
		t.Errorf("AllKinds has %d entries but %d distinct values; something is listed twice", len(AllKinds), len(listed))
	}

	// Resolve each declared constant name to its value via a lookup table
	// built from the package's own exported kinds.
	byName := map[string]Kind{
		"KindScanStarted": KindScanStarted, "KindScanFinished": KindScanFinished,
		"KindDocumentQueued": KindDocumentQueued, "KindDocumentReading": KindDocumentReading,
		"KindDocumentEmbedding": KindDocumentEmbedding, "KindDocumentIngested": KindDocumentIngested,
		"KindDocumentFailed": KindDocumentFailed, "KindDocumentDeleted": KindDocumentDeleted,
		"KindWorkerStateChanged": KindWorkerStateChanged, "KindRPCCompleted": KindRPCCompleted,
		"KindSnapshot": KindSnapshot,
	}
	for name := range declared {
		kind, known := byName[name]
		if !known {
			t.Errorf("Kind constant %s is new: add it to AllKinds and to this test's lookup table", name)
			continue
		}
		if !listed[kind] {
			t.Errorf("Kind constant %s (%q) is missing from AllKinds", name, kind)
		}
	}
}
