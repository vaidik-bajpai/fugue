package fugue_test

import (
	"strings"
	"testing"

	"github.com/vaidik-bajpai/fugue"
)

func toString(chars []rune) string {
	return string(chars)
}

func TestSingleReplicaInsertDelete(t *testing.T) {
	doc := fugue.NewFugueMaxSimple[rune]("alice")

	// Insert "Hello"
	input := []rune("Hello")
	msgs, err := doc.Insert(0, input...)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(msgs))
	}

	if toString(doc.Values()) != "Hello" {
		t.Fatalf("Expected 'Hello', got '%s'", toString(doc.Values()))
	}
	if doc.Length() != 5 {
		t.Fatalf("Expected length 5, got %d", doc.Length())
	}

	// Insert " World" at index 5
	_, err = doc.Insert(5, []rune(" World")...)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if toString(doc.Values()) != "Hello World" {
		t.Fatalf("Expected 'Hello World', got '%s'", toString(doc.Values()))
	}

	// Delete " World" starting at index 5, count 6
	_, err = doc.Delete(5, 6)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if toString(doc.Values()) != "Hello" {
		t.Fatalf("Expected 'Hello', got '%s'", toString(doc.Values()))
	}
}

func TestConcurrentEditsConvergence(t *testing.T) {
	// Replica A (Alice) and Replica B (Bob) start from shared initial state
	alice := fugue.NewFugueMaxSimple[rune]("alice")
	bob := fugue.NewFugueMaxSimple[rune]("bob")

	// Alice inserts "AC"
	aliceMsgs, _ := alice.Insert(0, []rune("AC")...)
	for _, msg := range aliceMsgs {
		_ = bob.ReceivePrimitive(msg)
	}

	if toString(alice.Values()) != "AC" || toString(bob.Values()) != "AC" {
		t.Fatalf("Initial sync failed")
	}

	// Alice inserts 'B' between A and C (at index 1) -> "ABC"
	aliceInsertOps, _ := alice.Insert(1, 'B')

	// Bob concurrently inserts 'X' between A and C (at index 1) -> "AXC"
	bobInsertOps, _ := bob.Insert(1, 'X')

	// Now sync ops across network
	for _, op := range aliceInsertOps {
		err := bob.ReceivePrimitive(op)
		if err != nil {
			t.Fatalf("Bob failed to receive Alice op: %v", err)
		}
	}

	for _, op := range bobInsertOps {
		err := alice.ReceivePrimitive(op)
		if err != nil {
			t.Fatalf("Alice failed to receive Bob op: %v", err)
		}
	}

	// Both replicas MUST converge to identical state
	aliceState := toString(alice.Values())
	bobState := toString(bob.Values())

	if aliceState != bobState {
		t.Fatalf("Replicas diverged! Alice: '%s', Bob: '%s'", aliceState, bobState)
	}
	t.Logf("Converged output: '%s'", aliceState)
}

func TestSaveAndLoad(t *testing.T) {
	doc1 := fugue.NewFugueMaxSimple[rune]("alice")
	_, _ = doc1.Insert(0, []rune("Local-First Collaborative Document")...)
	_, _ = doc1.Delete(6, 6) // Delete "-First" -> "Local Collaborative Document"

	snapshot, err := doc1.SavePrimitive()
	if err != nil {
		t.Fatalf("SavePrimitive failed: %v", err)
	}

	doc2 := fugue.NewFugueMaxSimple[rune]("bob")
	err = doc2.LoadPrimitive(snapshot)
	if err != nil {
		t.Fatalf("LoadPrimitive failed: %v", err)
	}

	if toString(doc1.Values()) != toString(doc2.Values()) {
		t.Fatalf("Loaded doc mismatch! Doc1: '%s', Doc2: '%s'", toString(doc1.Values()), toString(doc2.Values()))
	}
}

func TestMessageSerialization(t *testing.T) {
	alice := fugue.NewFugueMaxSimple[rune]("alice")
	msgs, err := alice.Insert(0, 'H')
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Marshal message to JSON bytes (as over network)
	msgBytes, err := msgs[0].MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// Unmarshal on Bob's side
	var receivedMsg fugue.Message[rune]
	err = receivedMsg.UnmarshalJSON(msgBytes)
	if err != nil {
		t.Fatalf("UnmarshalJSON failed: %v", err)
	}

	bob := fugue.NewFugueMaxSimple[rune]("bob")
	err = bob.ReceivePrimitive(receivedMsg)
	if err != nil {
		t.Fatalf("ReceivePrimitive failed: %v", err)
	}

	if string(bob.Values()) != "H" {
		t.Fatalf("Expected 'H', got '%s'", string(bob.Values()))
	}
}

func TestComplexConcurrentInterleaving(t *testing.T) {
	peerA := fugue.NewFugueMaxSimple[string]("peerA")
	peerB := fugue.NewFugueMaxSimple[string]("peerB")
	peerC := fugue.NewFugueMaxSimple[string]("peerC")

	// Peer A inserts "hello "
	opsA1, _ := peerA.Insert(0, "h", "e", "l", "l", "o", " ")

	// Broadcast A's initial ops to B and C
	for _, op := range opsA1 {
		_ = peerB.ReceivePrimitive(op)
		_ = peerC.ReceivePrimitive(op)
	}

	// Peer B inserts "world" at index 6
	opsB, _ := peerB.Insert(6, "w", "o", "r", "l", "d")

	// Peer C concurrently inserts "there " at index 6
	opsC, _ := peerC.Insert(6, "t", "h", "e", "r", "e", " ")

	// Exchange ops between all peers
	for _, op := range opsB {
		_ = peerA.ReceivePrimitive(op)
		_ = peerC.ReceivePrimitive(op)
	}
	for _, op := range opsC {
		_ = peerA.ReceivePrimitive(op)
		_ = peerB.ReceivePrimitive(op)
	}

	resA := strings.Join(peerA.Values(), "")
	resB := strings.Join(peerB.Values(), "")
	resC := strings.Join(peerC.Values(), "")

	if resA != resB || resB != resC {
		t.Fatalf("Three-way convergence failure! A='%s', B='%s', C='%s'", resA, resB, resC)
	}
	t.Logf("Three-way converged state: '%s'", resA)
}
