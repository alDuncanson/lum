package events

import (
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
