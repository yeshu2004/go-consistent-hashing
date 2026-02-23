package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
)

type ConsistentHashing struct {
	totalSlots uint64
	keys       []uint64
	nodes      []string
	mu         sync.RWMutex
}

func NewConsistentHashing(totalSlots uint64) *ConsistentHashing {
	return &ConsistentHashing{
		totalSlots: totalSlots,
		keys:       []uint64{},
		nodes:      []string{},
	}
}

// AddNode function adds a new node in the system i.e 
// hash space and node space returns the error
func (ch *ConsistentHashing) AddNode(node string) error {
	if len(node) == 0{
		return fmt.Errorf("node can't be a empty!")
	}
	key := ch.hashFunc(node)

	ch.mu.Lock()
	defer ch.mu.Unlock()

	index := findKeyIndex(key, ch.keys)
	if index < len(ch.keys) && ch.keys[index] == key {
		return fmt.Errorf("collision occured for node (%v)", node)
	}

	ch.keys = addKeyToIndex(key, index, ch.keys);
	ch.nodes = addNodeToIndex(node, index, ch.nodes);

	return nil
}

// Given an item GetNode function returns node 
// i.e. index it is associated within []nodes & error
func (ch *ConsistentHashing) GetNode(dataKey string) (string , error){
	if len(dataKey) == 0{
		return "", fmt.Errorf("key data cannot be empty");
	}

	ch.mu.RLock();
	defer ch.mu.RUnlock();

	if len(ch.keys) == 0{
		return "", fmt.Errorf("hash ring is empty")
	}

	key := ch.hashFunc(dataKey);

	index := findKeyIndex(key, ch.keys);

	if index == len(ch.keys) {
		index = 0
	}

	return ch.nodes[index], nil;
}

// RemoveNode removes the node from the hash space 
// and returns error.
func (ch *ConsistentHashing) RemoveNode(node string) error {
	if len(node) == 0{
		return fmt.Errorf("node can't be a empty!");
	}

	ch.mu.Lock();
	defer ch.mu.Unlock();

	if len(ch.keys) == 0{
		return fmt.Errorf("hash space is empty!")
	}

	key := ch.hashFunc(node);

	index := findKeyIndex(key, ch.keys);

	if index == len(ch.keys) || ch.keys[index] != key{
		return fmt.Errorf("node doesn't exists or doesn't match");
	}

	ch.keys = slices.Delete(ch.keys, index, index+1);
	ch.nodes = slices.Delete(ch.nodes, index, index+1);
	
	return nil;
}


// hashFunc creates an integer equivalent of a SHA256 hash and
// takes a modulo with the total number of slots in hash space
func (ch *ConsistentHashing) hashFunc(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	value := binary.BigEndian.Uint64(hash[:8])

	return value % uint64(ch.totalSlots)
}

func addNodeToIndex(node string, index int, nodes []string) []string {
	nodes = append(nodes, "")
	copy(nodes[index+1:], nodes[index:])
	nodes[index] = node

	return nodes;
}

func addKeyToIndex(key uint64, index int, keys []uint64) []uint64 {
	keys = append(keys, 0)
	copy(keys[index+1:], keys[index:])
	keys[index] = key

	return keys
}

// binary search over keys to find index of key
func findKeyIndex(key uint64, keys []uint64) int {
	l := 0
	u := len(keys)

	for l < u {
		m := l + (u-l)/2
		if keys[m] < key {
			l = m + 1
		} else {
			u = m
		}
	}

	return l
}
