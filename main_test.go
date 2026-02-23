package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestAddNode_Basic(t *testing.T) {
	ch := NewConsistentHashing(1024)
	if err := ch.AddNode("node1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ch.nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(ch.nodes))
	}
}

func TestAddNode_EmptyString(t *testing.T) {
	ch := NewConsistentHashing(1024)
	if err := ch.AddNode(""); err == nil {
		t.Fatal("expected error for empty node name")
	}
}

func TestAddNode_Duplicate(t *testing.T) {
	ch := NewConsistentHashing(1024)
	ch.AddNode("node1")
	if err := ch.AddNode("node1"); err == nil {
		t.Fatal("expected collision error for duplicate node")
	}
}

func TestAddNode_KeysRemainSorted(t *testing.T) {
	ch := NewConsistentHashing(1024)
	for _, n := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		ch.AddNode(n)
	}
	for i := 1; i < len(ch.keys); i++ {
		if ch.keys[i] <= ch.keys[i-1] {
			t.Errorf("keys not sorted at index %d", i)
		}
	}
}

func TestGetNode_EmptyKey(t *testing.T) {
	ch := NewConsistentHashing(1024)
	ch.AddNode("node1")
	if _, err := ch.GetNode(""); err == nil {
		t.Fatal("expected error for empty data key")
	}
}

func TestGetNode_EmptyRing(t *testing.T) {
	ch := NewConsistentHashing(1024)
	if _, err := ch.GetNode("somekey"); err == nil {
		t.Fatal("expected error when ring is empty")
	}
}

func TestGetNode_ReturnsValidNode(t *testing.T) {
	ch := NewConsistentHashing(1024)
	nodes := []string{"a", "b", "c"}
	for _, n := range nodes {
		ch.AddNode(n)
	}
	got, err := ch.GetNode("mydata")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, n := range nodes {
		if n == got {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("returned node %q not in the ring", got)
	}
}

func TestGetNode_WrapAroundReturnsNode(t *testing.T) {
	ch := NewConsistentHashing(1024)
	for _, n := range []string{"a", "b", "c"} {
		ch.AddNode(n)
	}
	for i := 0; i < 200; i++ {
		got, err := ch.GetNode(fmt.Sprintf("item-%d", i))
		if err != nil {
			t.Fatalf("GetNode failed: %v", err)
		}
		if got == "" {
			t.Fatalf("GetNode returned empty string for key item-%d (wrap-around bug)", i)
		}
	}
}

func TestGetNode_Consistent(t *testing.T) {
	ch := NewConsistentHashing(1024)
	for _, n := range []string{"node1", "node2", "node3"} {
		ch.AddNode(n)
	}
	first, _ := ch.GetNode("mydata")
	for i := 0; i < 20; i++ {
		got, _ := ch.GetNode("mydata")
		if got != first {
			t.Fatalf("inconsistent result: got %q, want %q", got, first)
		}
	}
}


func TestRemoveNode_Basic(t *testing.T) {
	ch := NewConsistentHashing(1024)
	ch.AddNode("node1")
	ch.AddNode("node2")
	if err := ch.RemoveNode("node1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ch.nodes) != 1 || ch.nodes[0] != "node2" {
		t.Fatalf("expected only node2, got %v", ch.nodes)
	}
}

func TestRemoveNode_EmptyString(t *testing.T) {
	ch := NewConsistentHashing(1024)
	if err := ch.RemoveNode(""); err == nil {
		t.Fatal("expected error for empty node name")
	}
}

func TestRemoveNode_EmptyRing(t *testing.T) {
	ch := NewConsistentHashing(1024)
	if err := ch.RemoveNode("node1"); err == nil {
		t.Fatal("expected error when ring is empty")
	}
}

func TestRemoveNode_NonExistent(t *testing.T) {
	ch := NewConsistentHashing(1024)
	ch.AddNode("node1")
	if err := ch.RemoveNode("ghost"); err == nil {
		t.Fatal("expected error when removing non-existent node")
	}
}

func TestRemoveNode_AllNodes(t *testing.T) {
	ch := NewConsistentHashing(1024)
	nodes := []string{"x", "y", "z"}
	for _, n := range nodes {
		ch.AddNode(n)
	}
	for _, n := range nodes {
		if err := ch.RemoveNode(n); err != nil {
			t.Fatalf("RemoveNode(%q) failed: %v", n, err)
		}
	}
	if len(ch.keys) != 0 {
		t.Fatal("ring should be empty after removing all nodes")
	}
}

func TestRemoveNode_GetNodeAfterRemoval(t *testing.T) {
	ch := NewConsistentHashing(1024)
	for _, n := range []string{"node1", "node2", "node3"} {
		ch.AddNode(n)
	}
	ch.RemoveNode("node2")
	got, err := ch.GetNode("somekey")
	if err != nil {
		t.Fatalf("unexpected error after removal: %v", err)
	}
	if got == "node2" {
		t.Fatal("removed node should not be returned by GetNode")
	}
}


func TestConcurrentAddAndGet(t *testing.T) {
	ch := NewConsistentHashing(1 << 16)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch.AddNode(fmt.Sprintf("node-%d", i))
		}(i)
	}
	wg.Wait()

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch.GetNode(fmt.Sprintf("key-%d", i))
		}(i)
	}
	wg.Wait()
}

func TestConcurrentAddRemove(t *testing.T) {
	ch := NewConsistentHashing(1 << 16)
	for i := 0; i < 50; i++ {
		ch.AddNode(fmt.Sprintf("pre-%d", i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			ch.AddNode(fmt.Sprintf("new-%d", i))
		}(i)
		go func(i int) {
			defer wg.Done()
			ch.RemoveNode(fmt.Sprintf("pre-%d", i))
		}(i)
	}
	wg.Wait()
}


func BenchmarkAddNode(b *testing.B) {
	ch := NewConsistentHashing(1 << 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}
}

func BenchmarkGetNode_10Nodes(b *testing.B) {
	ch := NewConsistentHashing(1 << 32)
	for i := 0; i < 10; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkGetNode_100Nodes(b *testing.B) {
	ch := NewConsistentHashing(1 << 32)
	for i := 0; i < 100; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkRemoveNode(b *testing.B) {
	ch := NewConsistentHashing(1 << 32)
	for i := 0; i < b.N; i++ {
		ch.AddNode(fmt.Sprintf("node-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.RemoveNode(fmt.Sprintf("node-%d", i))
	}
}