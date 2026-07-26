package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/vaidik-bajpai/fugue"
)

// DocumentReplica wraps a Fugue CRDT instance for text document editing.
type DocumentReplica struct {
	Name string
	CRDT *fugue.FugueMaxSimple[rune]
}

func NewDocumentReplica(name string) *DocumentReplica {
	return &DocumentReplica{
		Name: name,
		CRDT: fugue.NewFugueMaxSimple[rune](name),
	}
}

// Text returns the current text content of the document.
func (d *DocumentReplica) Text() string {
	return string(d.CRDT.Values())
}

// InsertText inserts text at a given character index and returns JSON-encoded operation messages to broadcast.
func (d *DocumentReplica) InsertText(index int, text string) [][]byte {
	chars := []rune(text)
	ops, err := d.CRDT.Insert(index, chars...)
	if err != nil {
		log.Fatalf("[%s] Insert error: %v", d.Name, err)
	}

	var payload [][]byte
	for _, op := range ops {
		data, err := json.Marshal(op)
		if err != nil {
			log.Fatalf("[%s] Serialization error: %v", d.Name, err)
		}
		payload = append(payload, data)
	}
	return payload
}

// DeleteText deletes count characters starting at index and returns JSON-encoded operation messages to broadcast.
func (d *DocumentReplica) DeleteText(index int, count int) [][]byte {
	ops, err := d.CRDT.Delete(index, count)
	if err != nil {
		log.Fatalf("[%s] Delete error: %v", d.Name, err)
	}

	var payload [][]byte
	for _, op := range ops {
		data, err := json.Marshal(op)
		if err != nil {
			log.Fatalf("[%s] Serialization error: %v", d.Name, err)
		}
		payload = append(payload, data)
	}
	return payload
}

// ReceiveRemoteOp applies an incoming JSON-encoded operation message received over the network.
func (d *DocumentReplica) ReceiveRemoteOp(opData []byte) {
	var op fugue.Message[rune]
	if err := json.Unmarshal(opData, &op); err != nil {
		log.Fatalf("[%s] Deserialization error: %v", d.Name, err)
	}
	if err := d.CRDT.ReceivePrimitive(op); err != nil {
		log.Fatalf("[%s] Apply op error: %v", d.Name, err)
	}
}

func main() {
	fmt.Println("=======================================================")
	fmt.Println("       Fugue CRDT Collaborative Text Editing Demo       ")
	fmt.Println("=======================================================")

	// 1. Initialize two replicas: Alice and Bob
	alice := NewDocumentReplica("Alice")
	bob := NewDocumentReplica("Bob")

	fmt.Printf("Initial state -> Alice: '%s' | Bob: '%s'\n\n", alice.Text(), bob.Text())

	// 2. Alice types "Hello World"
	fmt.Println("[Step 1] Alice types 'Hello World'...")
	aliceOps := alice.InsertText(0, "Hello World")
	fmt.Printf("Alice doc: '%s'\n", alice.Text())

	// Broadcast Alice's ops to Bob
	for _, op := range aliceOps {
		bob.ReceiveRemoteOp(op)
	}
	fmt.Printf("Bob syncs -> Bob doc: '%s'\n\n", bob.Text())

	// 3. Concurrent editing (Network split simulation):
	// Alice edits at index 5 (inserts ", Beautiful") -> "Hello, Beautiful World"
	// Bob edits at index 11 (inserts "!")           -> "Hello World!"
	fmt.Println("[Step 2] Concurrent offline edits:")
	fmt.Println("  - Alice inserts ', Beautiful' at index 5")
	aliceConcurrentOps := alice.InsertText(5, ", Beautiful")

	fmt.Println("  - Bob inserts '!' at index 11")
	bobConcurrentOps := bob.InsertText(11, "!")

	fmt.Printf("Before sync -> Alice: '%s' | Bob: '%s'\n\n", alice.Text(), bob.Text())

	// 4. Sync ops across the network
	fmt.Println("[Step 3] Exchanging network operation messages...")
	for _, op := range aliceConcurrentOps {
		bob.ReceiveRemoteOp(op)
	}
	for _, op := range bobConcurrentOps {
		alice.ReceiveRemoteOp(op)
	}

	fmt.Printf("After sync  -> Alice: '%s' | Bob: '%s'\n\n", alice.Text(), bob.Text())

	// 5. Deletion demo
	fmt.Println("[Step 4] Alice deletes ', Beautiful' (11 characters at index 5)")
	deleteOps := alice.DeleteText(5, 11)
	for _, op := range deleteOps {
		bob.ReceiveRemoteOp(op)
	}
	fmt.Printf("Final doc   -> Alice: '%s' | Bob: '%s'\n\n", alice.Text(), bob.Text())

	// 6. Snapshot Persistence (Save & Load)
	fmt.Println("[Step 5] Creating full state snapshot (Gzip compressed)...")
	snapshot, err := alice.CRDT.SavePrimitive()
	if err != nil {
		log.Fatalf("Save error: %v", err)
	}
	fmt.Printf("Compressed Snapshot Size: %d bytes\n", len(snapshot))

	// Load snapshot into a new replica, Charlie
	charlie := NewDocumentReplica("Charlie")
	if err := charlie.CRDT.LoadPrimitive(snapshot); err != nil {
		log.Fatalf("Load error: %v", err)
	}
	fmt.Printf("Charlie restored from snapshot -> Charlie doc: '%s'\n", charlie.Text())

	fmt.Println("\n=======================================================")
	fmt.Println("       Fugue CRDT Demo Completed Successfully!         ")
	fmt.Println("=======================================================")
}
