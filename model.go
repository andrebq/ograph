package ograph

import (
	"fmt"
	"github.com/andrebq/ograph/data"
)

type (
	RelationSet []*Relation

	// A node in the object graph
	Node struct {
		Attributes Attributes
		Gid        Nid
		Name       string
	}

	// Identity is used to allow other apis to refer to a given node
	// for later use.
	Identity struct {
		Gid Nid
	}

	// A connection between two nodes
	Relation struct {
		From       *Node
		To         *Node
		Name       string
		Attributes Attributes
	}

	// The attributes of a Node or Relation
	Attributes string

	// A Nid holds the information used to identify a node
	Nid uint64

	// The object graph
	G struct {
		repo *data.Repo
	}

	// A query used to walk the graph
	Query struct {
		g *G
		nodes []*Node
		err error
	}

	// Represents an error
	ApiError string

	// Used to describe if a Relation have some attribute, when nil always returns true
	Predicate func(*Relation) bool
)

func (n *Node) Rel(object string, predicate *Node) *Relation {
	return &Relation{
		From: n,
		To: predicate,
		Name: object,
	}
}

func (n *Node) Is(other interface{}) bool {
	if n == nil {
		return false
	}
	switch other := other.(type) {
	case int64:
		return n.Gid == Nid(other)
	case Nid:
		return n.Gid == other
	case Node:
		return n.Gid == other.Gid
	case *Node:
		return n.Gid == other.Gid
	default:
		return false
	}
}

func (p Predicate) Valid(r *Relation) bool {
	if p == nil {
		return true
	}
	return p(r)
}

// Error implements the error interface
func (a ApiError) Error() string {
	return string(a)
}

const (
	// ErrNotFound: Unable to find a Node or Relation in the graph
	ErrNotFound = ApiError("not found")

	// ErrInvalidEncoding: The attributes of a node aren't encoded as utf-8 objects
	ErrInvalidEncoding = ApiError("attributes must be a utf-8 encoded json")

	ErrAbortedByUser = ApiError("user aborted the transaction")

	// A Invalid Node id
	InvalidNid = Nid(0)
)

func (g *G) Use(repo *data.Repo) {
	g.repo = repo
}

func (g *G) SaveAll(what ...interface{}) error {
	g.repo.Begin()
	defer g.repo.End()
	for _, v := range what {
		err := g.save(v)
		if err != nil {
			return err
		}
	}
	return g.repo.Err()
}

func (g *G) save(what interface{}) error {
	switch what := what.(type) {
	case *Node:
		return g.saveNode(what)
	case *Relation:
		return g.saveRelation(what)
	default:
		return fmt.Errorf("cannot save %#q", what)
	}
}

func (g *G) saveNode(n *Node) error {
	var node data.Node
	node.Gid = uint64(n.Gid)
	node.Name = n.Name
	node.Attributes = string(n.Attributes)
	g.repo.SaveNode(&node)
	n.Gid = Nid(node.Gid)
	n.Attributes = Attributes(node.Attributes)
	return g.repo.Err()
}

func (g *G) saveRelation(r *Relation) error {
	var rel data.Relation
	rel.FromGid = uint64(r.From.Gid)
	rel.ToGid = uint64(r.To.Gid)
	rel.Attributes = string(r.Attributes)
	rel.Name = r.Name

	g.repo.SaveRelation(&rel)
	r.Attributes = Attributes(rel.Attributes)
	return g.repo.Err()
}

func (g *G) Node(id Nid, name string, out *Node) (*Node, error) {
	var tmpOut data.Node
	if err := g.repo.FetchNode(name, uint64(id), &tmpOut); err != nil {
		return nil, err
	}
	if out == nil {
		out = &Node{}
	}
	out.Gid = Nid(tmpOut.Gid)
	out.Name = tmpOut.Name
	out.Attributes = Attributes(tmpOut.Attributes)
	return out, nil
}

func (g *G) Walk(from *Node, using string) (RelationSet, error) {
	raw, err := g.repo.Walk(uint64(from.Gid), using, nil)
	if err != nil {
		return nil, err
	}
	nodes := make(map[uint64]*Node)
	out := make(RelationSet, len(raw))

	for i, r := range raw {
		fromN, ok := nodes[r.FromGid]
		if !ok {
			fromN = &Node{
				Gid: Nid(r.FromGid),
				Name: r.FromName,
				Attributes: Attributes(r.FromAttributes),
			}
			nodes[r.FromGid] = fromN
		}

		toN, ok := nodes[r.ToGid]
		if !ok {
			toN = &Node{
				Gid: Nid(r.ToGid),
				Name: r.ToName,
				Attributes: Attributes(r.ToAttributes),
			}
			nodes[r.ToGid] = toN
		}
		rel := &Relation{
			From: fromN,
			To: toN,
			Attributes: Attributes(r.Attributes),
			Name: r.Name,
		}
		out[i] = rel
	}
	return out, nil
}

func (g *G) Close() error {
	return g.repo.Close()
}
