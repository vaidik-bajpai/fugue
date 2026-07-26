package fugue

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const GZIP = true

var (
	ErrUnknownID       = errors.New("unknown ID")
	ErrIndexOutOfRange = errors.New("index out of range")
	ErrBadMessage      = errors.New("bad message")
)

// ID uniquely identifies a node in the Fugue tree by replica sender and counter.
type ID struct {
	Sender  string `json:"sender"`
	Counter int    `json:"counter"`
}

// Side specifies whether a child node is attached to the left or right of its parent.
type Side string

const (
	Left  Side = "L"
	Right Side = "R"
)

// Node represents a single element node in the Fugue CRDT tree.
type Node[T any] struct {
	id        ID
	value     *T
	isDeleted bool

	parent *Node[T]
	side   Side

	leftChildren  []*Node[T]
	rightChildren []*Node[T]

	size int

	hasRightOrigin bool
	rightOrigin    *Node[T]
}

// NewNode creates a root node for the Fugue tree.
func NewNode[T any]() *Node[T] {
	return &Node[T]{
		id: ID{
			Sender:  "",
			Counter: 0,
		},
		value:         nil,
		isDeleted:     true,
		parent:        nil,
		side:          Right,
		leftChildren:  []*Node[T]{},
		rightChildren: []*Node[T]{},
		size:          0,
	}
}

// MessageType indicates the operation type ("insert" or "delete").
type MessageType string

const (
	OpInsert MessageType = "insert"
	OpDelete MessageType = "delete"
)

// Message represents an operation message exchanged between peers.
type Message[T any] struct {
	Type           MessageType `json:"type"`
	ID             ID          `json:"id"`
	Value          *T          `json:"value,omitempty"`
	Parent         *ID         `json:"parent,omitempty"`
	Side           Side        `json:"side,omitempty"`
	HasRightOrigin bool        `json:"-"`
	RightOrigin    *ID         `json:"rightOrigin,omitempty"`
}

func (m Message[T]) MarshalJSON() ([]byte, error) {
	type Alias Message[T]
	aux := struct {
		Alias
		RightOrigin json.RawMessage `json:"rightOrigin,omitempty"`
	}{
		Alias: Alias(m),
	}

	if m.HasRightOrigin {
		if m.RightOrigin == nil {
			aux.RightOrigin = json.RawMessage("null")
		} else {
			b, err := json.Marshal(m.RightOrigin)
			if err != nil {
				return nil, err
			}
			aux.RightOrigin = b
		}
	}
	return json.Marshal(aux)
}

func (m *Message[T]) UnmarshalJSON(data []byte) error {
	type Alias Message[T]
	aux := struct {
		*Alias
		RightOrigin json.RawMessage `json:"rightOrigin,omitempty"`
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.RightOrigin != nil {
		m.HasRightOrigin = true
		if string(aux.RightOrigin) != "null" {
			var id ID
			if err := json.Unmarshal(aux.RightOrigin, &id); err != nil {
				return err
			}
			m.RightOrigin = &id
		} else {
			m.RightOrigin = nil
		}
	} else {
		m.HasRightOrigin = false
		m.RightOrigin = nil
	}
	return nil
}

// NodeSave represents the persistent JSON snapshot format of a Node.
type NodeSave[T any] struct {
	Value          *T    `json:"value"`
	IsDeleted      bool  `json:"isDeleted"`
	Parent         *ID   `json:"parent"`
	Side           Side  `json:"side"`
	Size           int   `json:"size"`
	HasRightOrigin bool  `json:"-"`
	RightOrigin    *ID   `json:"rightOrigin,omitempty"`
}

func (ns NodeSave[T]) MarshalJSON() ([]byte, error) {
	type Alias NodeSave[T]
	aux := struct {
		Alias
		RightOrigin json.RawMessage `json:"rightOrigin,omitempty"`
	}{
		Alias: Alias(ns),
	}

	if ns.HasRightOrigin {
		if ns.RightOrigin == nil {
			aux.RightOrigin = json.RawMessage("null")
		} else {
			b, err := json.Marshal(ns.RightOrigin)
			if err != nil {
				return nil, err
			}
			aux.RightOrigin = b
		}
	}
	return json.Marshal(aux)
}

func (ns *NodeSave[T]) UnmarshalJSON(data []byte) error {
	type Alias NodeSave[T]
	aux := struct {
		*Alias
		RightOrigin json.RawMessage `json:"rightOrigin,omitempty"`
	}{
		Alias: (*Alias)(ns),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.RightOrigin != nil {
		ns.HasRightOrigin = true
		if string(aux.RightOrigin) != "null" {
			var id ID
			if err := json.Unmarshal(aux.RightOrigin, &id); err != nil {
				return err
			}
			ns.RightOrigin = &id
		} else {
			ns.RightOrigin = nil
		}
	} else {
		ns.HasRightOrigin = false
		ns.RightOrigin = nil
	}
	return nil
}

// Tree holds the core Fugue tree representation.
type Tree[T any] struct {
	Root      *Node[T]
	NodesByID map[string][]*Node[T]
}

func NewTree[T any]() *Tree[T] {
	root := NewNode[T]()
	t := &Tree[T]{
		Root:      root,
		NodesByID: make(map[string][]*Node[T]),
	}
	t.NodesByID[""] = []*Node[T]{root}
	return t
}

func (t *Tree[T]) AddNode(id ID, value T, parent *Node[T], side Side, hasRightOrigin bool, rightOriginID *ID) (*Node[T], error) {
	valPtr := &value
	node := &Node[T]{
		id:             id,
		value:          valPtr,
		isDeleted:      false,
		parent:         parent,
		side:           side,
		leftChildren:   []*Node[T]{},
		rightChildren:  []*Node[T]{},
		size:           0,
		hasRightOrigin: hasRightOrigin,
	}

	if hasRightOrigin {
		if rightOriginID == nil {
			node.rightOrigin = nil
		} else {
			roNode, err := t.GetByID(*rightOriginID)
			if err != nil {
				return nil, err
			}
			node.rightOrigin = roNode
		}
	}

	bySender := t.NodesByID[id.Sender]
	bySender = append(bySender, node)
	t.NodesByID[id.Sender] = bySender

	t.insertIntoSiblings(node)
	t.UpdateSize(node, 1)

	return node, nil
}

func (t *Tree[T]) insertIntoSiblings(node *Node[T]) {
	parent := node.parent
	if parent == nil {
		return
	}

	if node.side == Right {
		rightSibs := parent.rightChildren
		i := 0
		for ; i < len(rightSibs); i++ {
			less := t.isLess(node.rightOrigin, rightSibs[i].rightOrigin)
			sameRO := node.rightOrigin == rightSibs[i].rightOrigin
			greaterSender := node.id.Sender > rightSibs[i].id.Sender

			if !(less || (sameRO && greaterSender)) {
				break
			}
		}
		rightSibs = append(rightSibs, nil)
		copy(rightSibs[i+1:], rightSibs[i:])
		rightSibs[i] = node
		parent.rightChildren = rightSibs
	} else {
		leftSibs := parent.leftChildren
		i := 0
		for ; i < len(leftSibs); i++ {
			if !(node.id.Sender > leftSibs[i].id.Sender) {
				break
			}
		}
		leftSibs = append(leftSibs, nil)
		copy(leftSibs[i+1:], leftSibs[i:])
		leftSibs[i] = node
		parent.leftChildren = leftSibs
	}
}

func (t *Tree[T]) isLess(a, b *Node[T]) bool {
	if a == b {
		return false
	}
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}

	aDepth := t.depth(a)
	bDepth := t.depth(b)

	aAnc := a
	bAnc := b

	if aDepth > bDepth {
		var lastSide Side
		for i := aDepth; i > bDepth; i-- {
			lastSide = aAnc.side
			aAnc = aAnc.parent
		}
		if aAnc == b {
			return lastSide == Left
		}
	}

	if bDepth > aDepth {
		var lastSide Side
		for i := bDepth; i > aDepth; i-- {
			lastSide = bAnc.side
			bAnc = bAnc.parent
		}
		if bAnc == a {
			return lastSide == Right
		}
	}

	for aAnc.parent != bAnc.parent {
		aAnc = aAnc.parent
		bAnc = bAnc.parent
	}

	if aAnc.side != bAnc.side {
		return aAnc.side == Left
	}

	var siblings []*Node[T]
	if aAnc.side == Left {
		siblings = aAnc.parent.leftChildren
	} else {
		siblings = aAnc.parent.rightChildren
	}

	return indexOf(siblings, aAnc) < indexOf(siblings, bAnc)
}

func indexOf[T any](slice []*Node[T], target *Node[T]) int {
	for i, item := range slice {
		if item == target {
			return i
		}
	}
	return -1
}

func (t *Tree[T]) depth(node *Node[T]) int {
	d := 0
	for curr := node; curr.parent != nil; curr = curr.parent {
		d++
	}
	return d
}

func (t *Tree[T]) UpdateSize(node *Node[T], delta int) {
	for anc := node; anc != nil; anc = anc.parent {
		anc.size += delta
	}
}

func (t *Tree[T]) GetByID(id ID) (*Node[T], error) {
	bySender, ok := t.NodesByID[id.Sender]
	if ok && id.Counter >= 0 && id.Counter < len(bySender) {
		node := bySender[id.Counter]
		if node != nil {
			return node, nil
		}
	}
	return nil, fmt.Errorf("%w: sender=%s counter=%d", ErrUnknownID, id.Sender, id.Counter)
}

func (t *Tree[T]) GetByIndex(node *Node[T], index int) (*Node[T], error) {
	if index < 0 || index >= node.size {
		return nil, fmt.Errorf("%w: index=%d size=%d", ErrIndexOutOfRange, index, node.size)
	}

	remaining := index
Outer:
	for {
		for _, child := range node.leftChildren {
			if remaining < child.size {
				node = child
				continue Outer
			}
			remaining -= child.size
		}
		if !node.isDeleted {
			if remaining == 0 {
				return node, nil
			}
			remaining--
		}
		for _, child := range node.rightChildren {
			if remaining < child.size {
				node = child
				continue Outer
			}
			remaining -= child.size
		}
		return nil, errors.New("index in range but not found")
	}
}

func (t *Tree[T]) LeftmostDescendant(node *Node[T]) *Node[T] {
	desc := node
	for len(desc.leftChildren) != 0 {
		desc = desc.leftChildren[0]
	}
	return desc
}

func (t *Tree[T]) NextNonDescendant(node *Node[T]) *Node[T] {
	curr := node
	for curr.parent != nil {
		var siblings []*Node[T]
		if curr.side == Left {
			siblings = curr.parent.leftChildren
		} else {
			siblings = curr.parent.rightChildren
		}
		idx := indexOf(siblings, curr)
		if idx < len(siblings)-1 {
			nextSibling := siblings[idx+1]
			return t.LeftmostDescendant(nextSibling)
		} else if curr.side == Left {
			return curr.parent
		}
		curr = curr.parent
	}
	return nil
}

type frame struct {
	side       Side
	childIndex int
}

func (t *Tree[T]) Traverse(node *Node[T], yieldFunc func(val T) bool) {
	curr := node
	stack := []frame{
		{side: Left, childIndex: 0},
	}

	for {
		top := &stack[len(stack)-1]
		var children []*Node[T]
		if top.side == Left {
			children = curr.leftChildren
		} else {
			children = curr.rightChildren
		}

		if top.childIndex == len(children) {
			if top.side == Left {
				if !curr.isDeleted {
					if !yieldFunc(*curr.value) {
						return
					}
				}
				top.side = Right
				top.childIndex = 0
			} else {
				if curr.parent == nil {
					return
				}
				curr = curr.parent
				stack = stack[:len(stack)-1]
			}
		} else {
			child := children[top.childIndex]
			top.childIndex++
			if child.size > 0 {
				curr = child
				stack = append(stack, frame{side: Left, childIndex: 0})
			}
		}
	}
}

func (t *Tree[T]) Save() ([]byte, error) {
	save := make(map[string][]NodeSave[T])

	for sender, bySender := range t.NodesByID {
		nodeSaves := make([]NodeSave[T], len(bySender))
		for i, node := range bySender {
			var parentID *ID
			if node.parent != nil {
				id := node.parent.id
				parentID = &id
			}
			ns := NodeSave[T]{
				Value:          node.value,
				IsDeleted:      node.isDeleted,
				Parent:         parentID,
				Side:           node.side,
				Size:           node.size,
				HasRightOrigin: node.hasRightOrigin,
			}
			if node.hasRightOrigin {
				if node.rightOrigin != nil {
					roID := node.rightOrigin.id
					ns.RightOrigin = &roID
				}
			}
			nodeSaves[i] = ns
		}
		save[sender] = nodeSaves
	}
	return json.Marshal(save)
}

func (t *Tree[T]) Load(saveData []byte) error {
	var save map[string][]NodeSave[T]
	if err := json.Unmarshal(saveData, &save); err != nil {
		return err
	}

	t.NodesByID = make(map[string][]*Node[T])
	t.Root = NewNode[T]()

	// Pass 1: recreate all nodes without pointers
	for sender, bySenderSave := range save {
		if sender == "" {
			t.Root.size = bySenderSave[0].Size
			t.NodesByID[""] = []*Node[T]{t.Root}
			continue
		}
		nodes := make([]*Node[T], len(bySenderSave))
		for counter, nodeSave := range bySenderSave {
			nodes[counter] = &Node[T]{
				id: ID{
					Sender:  sender,
					Counter: counter,
				},
				value:          nodeSave.Value,
				isDeleted:      nodeSave.IsDeleted,
				side:           nodeSave.Side,
				size:           nodeSave.Size,
				leftChildren:   []*Node[T]{},
				rightChildren:  []*Node[T]{},
				hasRightOrigin: nodeSave.HasRightOrigin,
			}
		}
		t.NodesByID[sender] = nodes
	}

	// Pass 2: fill parent and rightOrigin pointers
	for sender, bySender := range t.NodesByID {
		if sender == "" {
			continue
		}
		bySenderSave := save[sender]
		for i, node := range bySender {
			nodeSave := bySenderSave[i]
			if nodeSave.Parent != nil {
				parent, err := t.GetByID(*nodeSave.Parent)
				if err != nil {
					return fmt.Errorf("load parent error: %w", err)
				}
				node.parent = parent
			}
			if nodeSave.HasRightOrigin {
				if nodeSave.RightOrigin != nil {
					ro, err := t.GetByID(*nodeSave.RightOrigin)
					if err != nil {
						return fmt.Errorf("load rightOrigin error: %w", err)
					}
					node.rightOrigin = ro
				} else {
					node.rightOrigin = nil
				}
			}
		}
	}

	// Pass 3: topological sibling insertion order based on rightOrigin dependencies
	readyNodes := []*Node[T]{}
	pendingNodes := make(map[*Node[T]][]*Node[T])

	for sender, bySender := range t.NodesByID {
		if sender == "" {
			continue
		}
		for _, node := range bySender {
			if !node.hasRightOrigin || node.rightOrigin == nil {
				readyNodes = append(readyNodes, node)
			} else {
				pendingNodes[node.rightOrigin] = append(pendingNodes[node.rightOrigin], node)
			}
		}
	}

	for len(readyNodes) > 0 {
		node := readyNodes[len(readyNodes)-1]
		readyNodes = readyNodes[:len(readyNodes)-1]

		t.insertIntoSiblings(node)

		if deps, ok := pendingNodes[node]; ok {
			readyNodes = append(readyNodes, deps...)
			delete(pendingNodes, node)
		}
	}

	if len(pendingNodes) > 0 {
		return errors.New("internal error: failed to validate all nodes during load")
	}

	return nil
}

// FugueMaxSimple is the main Fugue CRDT container matching FugueMaxSimple in TypeScript.
type FugueMaxSimple[T any] struct {
	counter   int
	replicaID string
	tree      *Tree[T]
}

func NewFugueMaxSimple[T any](replicaID string) *FugueMaxSimple[T] {
	return &FugueMaxSimple[T]{
		counter:   0,
		replicaID: replicaID,
		tree:      NewTree[T](),
	}
}

func (f *FugueMaxSimple[T]) ReplicaID() string {
	return f.replicaID
}

func (f *FugueMaxSimple[T]) Insert(index int, values ...T) ([]Message[T], error) {
	msgs := make([]Message[T], len(values))
	for i, val := range values {
		msg, err := f.insertOne(index+i, val)
		if err != nil {
			return nil, err
		}
		msgs[i] = msg
	}
	return msgs, nil
}

func (f *FugueMaxSimple[T]) insertOne(index int, value T) (Message[T], error) {
	id := ID{
		Sender:  f.replicaID,
		Counter: f.counter,
	}
	f.counter++

	var leftOrigin *Node[T]
	var err error
	if index == 0 {
		leftOrigin = f.tree.Root
	} else {
		leftOrigin, err = f.tree.GetByIndex(f.tree.Root, index-1)
		if err != nil {
			return Message[T]{}, err
		}
	}

	var msg Message[T]
	if len(leftOrigin.rightChildren) == 0 {
		msg = Message[T]{
			Type:   OpInsert,
			ID:     id,
			Value:  &value,
			Parent: &leftOrigin.id,
			Side:   Right,
		}
		rightOrigin := f.tree.NextNonDescendant(leftOrigin)
		msg.HasRightOrigin = true
		if rightOrigin != nil {
			roID := rightOrigin.id
			msg.RightOrigin = &roID
		} else {
			msg.RightOrigin = nil
		}
	} else {
		rightOrigin := f.tree.LeftmostDescendant(leftOrigin.rightChildren[0])
		roID := rightOrigin.id
		msg = Message[T]{
			Type:   OpInsert,
			ID:     id,
			Value:  &value,
			Parent: &roID,
			Side:   Left,
		}
	}

	if err := f.ReceivePrimitive(msg); err != nil {
		return Message[T]{}, err
	}

	return msg, nil
}

func (f *FugueMaxSimple[T]) Delete(startIndex int, count int) ([]Message[T], error) {
	msgs := make([]Message[T], count)
	for i := 0; i < count; i++ {
		msg, err := f.deleteOne(startIndex)
		if err != nil {
			return nil, err
		}
		msgs[i] = msg
	}
	return msgs, nil
}

func (f *FugueMaxSimple[T]) deleteOne(index int) (Message[T], error) {
	node, err := f.tree.GetByIndex(f.tree.Root, index)
	if err != nil {
		return Message[T]{}, err
	}

	msg := Message[T]{
		Type: OpDelete,
		ID:   node.id,
	}

	if err := f.ReceivePrimitive(msg); err != nil {
		return Message[T]{}, err
	}

	return msg, nil
}

func (f *FugueMaxSimple[T]) ReceivePrimitive(msg Message[T]) error {
	switch msg.Type {
	case OpInsert:
		if msg.Parent == nil {
			return errors.New("insert message missing parent")
		}
		if msg.Value == nil {
			return errors.New("insert message missing value")
		}
		parent, err := f.tree.GetByID(*msg.Parent)
		if err != nil {
			return fmt.Errorf("insert parent lookup failed: %w", err)
		}
		_, err = f.tree.AddNode(msg.ID, *msg.Value, parent, msg.Side, msg.HasRightOrigin, msg.RightOrigin)
		if err != nil {
			return err
		}
	case OpDelete:
		node, err := f.tree.GetByID(msg.ID)
		if err != nil {
			return fmt.Errorf("delete node lookup failed: %w", err)
		}
		if !node.isDeleted {
			node.value = nil
			node.isDeleted = true
			f.tree.UpdateSize(node, -1)
		}
	default:
		return fmt.Errorf("%w: %s", ErrBadMessage, msg.Type)
	}
	return nil
}

func (f *FugueMaxSimple[T]) Get(index int) (T, error) {
	var zero T
	if index < 0 || index >= f.Length() {
		return zero, fmt.Errorf("%w: index %d", ErrIndexOutOfRange, index)
	}
	node, err := f.tree.GetByIndex(f.tree.Root, index)
	if err != nil {
		return zero, err
	}
	if node.value == nil {
		return zero, errors.New("node has no value")
	}
	return *node.value, nil
}

func (f *FugueMaxSimple[T]) Values() []T {
	var res []T
	f.tree.Traverse(f.tree.Root, func(val T) bool {
		res = append(res, val)
		return true
	})
	return res
}

func (f *FugueMaxSimple[T]) Length() int {
	return f.tree.Root.size
}

func (f *FugueMaxSimple[T]) SavePrimitive() ([]byte, error) {
	bytes, err := f.tree.Save()
	if err != nil {
		return nil, err
	}
	if GZIP {
		return gzipCompress(bytes)
	}
	return bytes, nil
}

func (f *FugueMaxSimple[T]) LoadPrimitive(savedState []byte) error {
	if len(savedState) == 0 {
		return nil
	}
	var data []byte = savedState
	var err error
	if GZIP {
		data, err = gzipDecompress(savedState)
		if err != nil {
			return fmt.Errorf("gzip decompress error: %w", err)
		}
	}
	return f.tree.Load(data)
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}
